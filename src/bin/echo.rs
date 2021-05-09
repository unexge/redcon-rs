use redcon::{listen, Conn, Type};

#[tokio::main]
async fn main() {
    listen("127.0.0.1:6379", |conn: Conn, cmd: Type| async move {
        match cmd {
            Type::SimpleString(str) => {
                conn.write_simple_string(str).await.unwrap();
            }
            Type::Error(err) => {
                conn.write_error(err).await.unwrap();
            }
            Type::Integer(num) => {
                conn.write_integer(num).await.unwrap();
            }
            Type::BulkString(str) => {
                conn.write_bulk_string(str).await.unwrap();
            }
            Type::Null => {
                conn.write_null().await.unwrap();
            }
            Type::Array(arr) => {
                conn.write_array(arr).await.unwrap();
            }
        }
    })
    .await
    .expect("could not listen");
}
