use redcon::{listen, Command, Conn, Type};

#[tokio::main]
async fn main() {
    listen("127.0.0.1:6379", |conn: Conn, cmd: Command| async move {
        conn.write_array(cmd.into_iter().map(Type::BulkString).collect())
            .await
            .unwrap();
    })
    .await
    .expect("could not listen");
}
