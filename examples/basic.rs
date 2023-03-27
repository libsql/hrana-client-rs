use hrana_client::{
    proto::{Stmt, Value},
    Client,
};

#[tokio::main]
async fn main() {
    // Open a `hrana.Client`, which works like a connection pool in standard SQL
    // databases, but it uses just a single network connection internally
    let url = std::env::var("URL").unwrap_or_else(|_| String::from("localhost:2023"));
    let jwt = std::env::var("JWT").unwrap_or_default();
    let (client, fut) = Client::connect(&url, Some(jwt)).await.unwrap();

    // Open a `hrana.Stream`, which is an interactive SQL stream. This corresponds
    // to a "connection" from other SQL databases
    let stream = client.open_stream().await.unwrap();
    println!("Stream opened");

    // Fetch all rows returned by a SQL statement
    let books = stream
        .execute(Stmt::new(
            "SELECT title, year FROM book WHERE author = 'Jane Austen'".to_string(),
            true,
        ))
        .await
        .unwrap();
    for book in books.rows.iter() {
        println!("{:?}", book);
    }

    let steps = stream
        .execute(Stmt::new("EXPLAIN SELECT * FROM book".to_string(), true))
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
    client.shutdown().await.unwrap();

    fut.await.unwrap();
}
