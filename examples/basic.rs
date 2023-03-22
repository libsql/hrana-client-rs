use hrana_client::{proto::Value, Client};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    // Open a `hrana.Client`, which works like a connection pool in standard SQL
    // databases, but it uses just a single network connection internally
    let url = std::env::var("URL").unwrap_or_else(|_| String::from("localhost:2023"));
    let jwt = std::env::var("JWT").unwrap_or_default();
    let mut client = Client::open(&url, &jwt).await.unwrap();

    // Open a `hrana.Stream`, which is an interactive SQL stream. This corresponds
    // to a "connection" from other SQL databases
    let mut stream = client.open_stream().await.unwrap();
    println!("Stream opened");

    // Fetch all rows returned by a SQL statement
    let books = stream
        .execute("SELECT title, year FROM book WHERE author = 'Jane Austen'")
        .await
        .unwrap();
    for book in books.rows.iter() {
        println!("{:?}", book);
    }

    let steps = stream
        .execute("EXPLAIN SELECT * FROM book")
        .await
        .unwrap()
        .rows
        .iter()
        .map(|step| {
            if let Some(Value::Text { value: step }) = step.get(1) {
                step
            } else {
                ""
            }
            .to_owned()
        })
        .collect::<Vec<String>>()
        .join(", ");
    println!("Steps: {steps}");

    // When you are done, remember to close the client
    client.close().await.unwrap();
}
