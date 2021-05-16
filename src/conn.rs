use std::future::Future;
use std::str;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{BufReader, BufWriter};
use tokio::sync::Mutex;
use tokio::{net::tcp::OwnedWriteHalf, net::TcpListener};

use crate::resp::{Error, Type};

#[derive(Clone, Debug)]
pub struct Conn {
    // TODO: is it possible without mutex?
    // TODO: maket it generic over writer?
    writer: Arc<Mutex<BufWriter<OwnedWriteHalf>>>,
}

impl Conn {
    pub fn new(writer: OwnedWriteHalf) -> Self {
        let writer = Arc::new(Mutex::new(BufWriter::new(writer)));
        Self { writer }
    }

    pub async fn write_simple_string(&self, str: String) -> Result<()> {
        let mut writer = self.writer.lock().await;
        Type::SimpleString(str).write(&mut *writer).await?;
        Ok(())
    }

    pub async fn write_error(&self, err: String) -> Result<()> {
        let mut writer = self.writer.lock().await;
        Type::Error(err).write(&mut *writer).await?;
        Ok(())
    }

    pub async fn write_integer(&self, num: i64) -> Result<()> {
        let mut writer = self.writer.lock().await;
        Type::Integer(num).write(&mut *writer).await?;
        Ok(())
    }

    pub async fn write_bulk_string(&self, str: String) -> Result<()> {
        let mut writer = self.writer.lock().await;
        Type::BulkString(str).write(&mut *writer).await?;
        Ok(())
    }

    pub async fn write_null(&self) -> Result<()> {
        let mut writer = self.writer.lock().await;
        Type::Null.write(&mut *writer).await?;
        Ok(())
    }

    pub async fn write_array(&self, arr: Vec<Type>) -> Result<()> {
        let mut writer = self.writer.lock().await;
        Type::Array(arr).write(&mut *writer).await?;
        Ok(())
    }
}

pub async fn listen<Handler, Fut>(addr: &str, handler: Handler) -> Result<()>
where
    Handler: Fn(Conn, Type) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    let handler = Arc::new(handler);

    loop {
        let (socket, _) = listener.accept().await?;
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            let (read, write) = socket.into_split();
            let mut read = BufReader::new(read);
            let conn = Conn::new(write);

            loop {
                let cmd = match Type::read(&mut read).await {
                    Ok(it) => it,
                    Err(err) => {
                        if let Some(Error::UnexpectedEof) = err.downcast_ref::<Error>() {
                            break;
                        }
                        eprintln!("could not read command: {}", err);
                        continue;
                    }
                };

                let conn = conn.clone();
                let handler = Arc::clone(&handler);
                tokio::spawn(handler(conn, cmd));
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::BufStream;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use tokio::time::{sleep, timeout};

    use super::*;

    struct Server {
        _shutdown_tx: oneshot::Sender<()>,
    }

    async fn server_and_client<Handler, Fut>(
        addr: &'static str,
        handler: Handler,
    ) -> Result<(Server, BufStream<TcpStream>)>
    where
        Handler: Fn(Conn, Type) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            tokio::select! {
                res = listen(addr, handler) => {
                    if let Err(err) = res {
                        panic!("could not listen: {}", err);
                    };
                },
                _ = shutdown_rx => {},
            };
        });

        // FIXME: wait for listening signal from Server.
        sleep(Duration::from_millis(10)).await;

        let client = timeout(Duration::from_millis(10), TcpStream::connect(addr)).await??;
        let client = BufStream::new(client);

        Ok((
            Server {
                _shutdown_tx: shutdown_tx,
            },
            client,
        ))
    }

    #[tokio::test]
    async fn accept_connections() -> Result<()> {
        let (_server, mut client) =
            server_and_client("127.0.0.1:6379", |conn: Conn, cmd: Type| async move {
                // FIXME: this panic is not propagated.
                assert!(matches!(cmd, Type::SimpleString(cmd) if cmd == "ping"));
                conn.write_simple_string("pong".to_string()).await.unwrap();
            })
            .await?;

        Type::SimpleString("ping".to_string())
            .write(&mut client)
            .await?;

        assert_eq!(
            Type::read(&mut client).await?,
            Type::SimpleString("pong".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_conn() -> Result<()> {
        let (_server, mut client) =
            server_and_client("127.0.0.1:6380", |conn: Conn, _cmd: Type| async move {
                conn.write_simple_string("simple string".to_string())
                    .await
                    .unwrap();
                conn.write_error("error".to_string()).await.unwrap();
                conn.write_integer(42).await.unwrap();
                conn.write_bulk_string("bulk string".to_string())
                    .await
                    .unwrap();
                conn.write_null().await.unwrap();
                conn.write_array(vec![Type::Null, Type::Integer(42)])
                    .await
                    .unwrap();
            })
            .await?;

        Type::SimpleString("start".to_string())
            .write(&mut client)
            .await?;

        assert_eq!(
            Type::read(&mut client).await?,
            Type::SimpleString("simple string".to_string())
        );
        assert_eq!(
            Type::read(&mut client).await?,
            Type::Error("error".to_string())
        );
        assert_eq!(Type::read(&mut client).await?, Type::Integer(42));
        assert_eq!(
            Type::read(&mut client).await?,
            Type::BulkString("bulk string".to_string())
        );
        assert_eq!(Type::read(&mut client).await?, Type::Null);
        assert_eq!(
            Type::read(&mut client).await?,
            Type::Array(vec![Type::Null, Type::Integer(42)])
        );

        Ok(())
    }
}
