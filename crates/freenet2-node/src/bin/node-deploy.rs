#[tokio::main]
async fn main() {
    let start = async { println!("started") };
    start.await;
}
