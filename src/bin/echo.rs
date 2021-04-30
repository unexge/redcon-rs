use redcon::listen;

#[tokio::main]
async fn main() {
    listen("127.0.0.1:6379", |cmd| cmd)
        .await
        .expect("could not listen");
}
