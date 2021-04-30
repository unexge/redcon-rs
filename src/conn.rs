use std::error::Error as StdError;
use std::str;
use std::sync::Arc;

use tokio::io::BufStream;
use tokio::net::TcpListener;

use crate::resp::{Error, Type};

pub async fn listen<Handler>(addr: &str, handler: Handler) -> Result<(), Box<dyn StdError>>
where
    // TODO: add &mut Conn and make it async.
    Handler: Fn(Type) -> Type + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    let handler = Arc::new(handler);

    loop {
        let (socket, _) = listener.accept().await?;
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            let mut socket = BufStream::new(socket);

            loop {
                let cmd = match Type::read(&mut socket).await {
                    Ok(it) => it,
                    Err(err) => {
                        if let Some(Error::UnexpectedEof) = err.downcast_ref::<Error>() {
                            break;
                        }
                        eprintln!("could not read command: {}", err);
                        continue;
                    }
                };

                let response = handler(cmd);
                if let Err(err) = response.write(&mut socket).await {
                    eprintln!("could not write response to client: {}", err);
                    continue;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout};

    use super::*;

    #[tokio::test]
    async fn accept_connections() -> Result<(), Box<dyn StdError>> {
        tokio::spawn(async {
            listen("127.0.0.1:6379", |cmd: Type| -> Type {
                assert!(matches!(cmd, Type::SimpleString(cmd) if cmd == "ping"));
                Type::SimpleString("pong".to_string())
            })
            .await
            .expect("could not listen");
        });

        // TODO: remove?
        sleep(Duration::from_millis(10)).await;

        let client = timeout(
            Duration::from_millis(10),
            TcpStream::connect("127.0.0.1:6379"),
        )
        .await??;
        let mut client = tokio::io::BufStream::new(client);

        Type::SimpleString("ping".to_string())
            .write(&mut client)
            .await?;

        assert_eq!(
            Type::read(&mut client).await?,
            Type::SimpleString("pong".to_string())
        );

        Ok(())
    }
}
