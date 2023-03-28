use hrana_client::{
    proto::Stmt,
    Client,
};

#[tokio::main]
async fn main() {
    // Open a `hrana.Client`, which works like a connection pool in standard SQL
    // databases, but it uses just a single network connection internally
    let url = std::env::var("URL").unwrap_or_else(|_| String::from("ws://localhost:2023"));
    let jwt = std::env::var("JWT").unwrap_or_default();
    let (client, fut) = Client::connect(&url, Some(jwt)).await.unwrap();

    // Open a `hrana.Stream`, which is an interactive SQL stream. This corresponds
    // to a "connection" from other SQL databases
    let stream = client.open_stream().await.unwrap();
    println!("Stream opened");

    let mut batch = hrana_client::proto::Batch::new();

    batch.step(
        None,
        Stmt::new("CREATE TABLE IF NOT EXISTS test (id INT, name TEXT)", true),
    );
    batch.step(None, Stmt::new("INSERT INTO test (id, name) VALUES (1, 2)", true));
    batch.step(None, Stmt::new("INSERT INTO test (id, name) VALUES (2, 3)", true));
    batch.step(None, Stmt::new("SELECT * FROM test", true));

    let resp = stream.execute_batch(batch).await.unwrap();
    println!("Batch executed: {:?}", resp);

    // When you are done, remember to close the client
    client.shutdown().await.unwrap();

    fut.await.unwrap();
}
