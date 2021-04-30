use std::fmt;

use anyhow::{anyhow, bail, Result};
use async_recursion::async_recursion;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter,
};

#[derive(Debug)]
pub enum Error {
    UnexpectedEof,
    ExpectedLine,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::UnexpectedEof => write!(f, "unexpected eof"),
            Error::ExpectedLine => write!(f, "expected line"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Null,
    Array(Vec<Type>),
}

impl Type {
    pub async fn write(self, dst: impl AsyncWrite + Unpin + Send) -> Result<()> {
        let mut dst = BufWriter::new(dst);
        self.write_buf(&mut dst).await?;
        dst.flush().await?;
        Ok(())
    }

    #[async_recursion]
    async fn write_buf(self, dst: &mut BufWriter<impl AsyncWrite + Unpin + Send>) -> Result<()> {
        async fn write_line(
            dst: &mut BufWriter<impl AsyncWrite + Unpin + Send>,
            tag: u8,
            buf: &[u8],
        ) -> Result<()> {
            dst.write_u8(tag).await?;
            dst.write_all(buf).await?;
            dst.write_all(&[b'\r', b'\n']).await?;
            Ok(())
        }

        match self {
            Self::SimpleString(s) => {
                write_line(dst, b'+', s.as_bytes()).await?;
            }
            Self::Error(s) => {
                write_line(dst, b'-', s.as_bytes()).await?;
            }
            Self::Integer(n) => {
                write_line(dst, b':', n.to_string().as_bytes()).await?;
            }
            Self::BulkString(s) => {
                let buf = s.as_bytes();

                write_line(dst, b'$', buf.len().to_string().as_bytes()).await?;

                dst.write_all(buf).await?;
                dst.write_all(&[b'\r', b'\n']).await?;
            }
            Self::Array(elements) => {
                write_line(dst, b'*', elements.len().to_string().as_bytes()).await?;

                for elem in elements {
                    elem.write_buf(dst).await?;
                }
            }
            Self::Null => {
                write_line(dst, b'$', b"-1").await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    pub async fn read(src: &mut (impl AsyncBufRead + Unpin + Send)) -> Result<Self> {
        async fn read_line(src: &mut (impl AsyncBufRead + Unpin + Send)) -> Result<String> {
            let mut buf = vec![];
            match src.read_until(b'\n', &mut buf).await {
                Ok(0) => bail!(Error::UnexpectedEof),
                Ok(_) => (),
                Err(err) => bail!(err),
            };

            let len = buf.len();
            if len < 2 || buf[(len - 2)..] != [b'\r', b'\n'] {
                bail!(Error::ExpectedLine)
            }

            // FIXME: use from_utf8_lossy?
            String::from_utf8(buf[..(len - 2)].into())
                .map_err(|err| anyhow!("expected utf-8: {}", err))
        }

        let line = read_line(src).await?;

        match line.as_bytes().first() {
            // FIXME: is str to String allocates?
            Some(b'+') => Ok(Self::SimpleString(line[1..].into())),
            Some(b'-') => Ok(Self::Error(line[1..].into())),
            Some(b':') => Ok(Self::Integer(line[1..].parse()?)),
            Some(b'$') => {
                if line == "$-1" {
                    return Ok(Self::Null);
                }

                let len: usize = line[1..].parse()?;
                let mut buf = vec![0; len + 2];
                src.read_exact(&mut buf).await?;

                if buf[len..] != [b'\r', b'\n'] {
                    bail!(Error::ExpectedLine)
                }

                Ok(Self::BulkString(String::from_utf8(buf[..len].into())?))
            }
            Some(b'*') => {
                if line == "*-1" {
                    return Ok(Self::Null);
                }

                let len: usize = line[1..].parse()?;
                let mut res = Vec::with_capacity(len);
                for _ in 0..len {
                    res.push(Self::read(src).await?);
                }

                Ok(Self::Array(res))
            }
            _ => bail!("unknown type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{duplex, BufReader};

    use super::*;

    macro_rules! type_tests {
        ($($str:expr => $ty:expr,)*) => {
            #[tokio::test]
            async fn read() -> Result<()> {
                $(
                    assert_eq!(
                        Type::read(&mut $str.to_vec().as_slice()).await?,
                        $ty
                    );
                )*
                Ok(())
            }

            #[tokio::test]
            async fn write() -> Result<()> {
                $(
                    let mut buf = vec![];
                    $ty.write(&mut buf).await?;
                    assert_eq!(buf, $str);
                )*
                Ok(())
            }

            #[tokio::test]
            async fn write_and_read() -> Result<()> {
                let (mut write, read) = duplex(8096);
                let mut read = BufReader::new(read);
                $(
                    $ty.clone().write(&mut write).await?;
                    assert_eq!(Type::read(&mut read).await?, $ty);
                )*
                Ok(())
            }
        }
    }

    type_tests! {
        b"+hello world\r\n" => Type::SimpleString("hello world".to_string()),
        b"-error message\r\n" => Type::Error("error message".to_string()),
        b":1000\r\n" => Type::Integer(1000),
        b"$11\r\nhello world\r\n" => Type::BulkString("hello world".to_string()),
        b"$-1\r\n" => Type::Null,
        b"*2\r\n+hello world\r\n$11\r\nhello world\r\n" => Type::Array(vec![
            Type::SimpleString("hello world".to_string()),
            Type::BulkString("hello world".to_string()),
        ]),
    }

    #[tokio::test]
    async fn null_array() -> Result<()> {
        assert_eq!(
            Type::read(&mut b"*-1\r\n".to_vec().as_slice()).await?,
            Type::Null,
        );
        Ok(())
    }
}
