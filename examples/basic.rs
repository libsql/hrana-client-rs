use hrana_client::{Client, Stream};
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    // Open a `hrana.Client`, which works like a connection pool in standard SQL
    // databases, but it uses just a single network connection internally
    let url = std::env::var("URL").unwrap_or_else(|_| String::from("localhost:2023"));
    let jwt = std::env::var("JWT").unwrap_or_default();
    let client = Arc::new(Mutex::new(Client::open(&url, &jwt).await.unwrap()));

    // Open a `hrana.Stream`, which is an interactive SQL stream. This corresponds
    // to a "connection" from other SQL databases
    let mut stream = Client::open_stream(client.clone()).await.unwrap();

    // Fetch all rows returned by a SQL statement
    let books = stream
        .query("SELECT title, year FROM book WHERE author = 'Jane Austen'")
        .await
        .unwrap();
    for book in books.iter() {
        println!("{:?}", book);
    }

    /*
        // Fetch a single row
        let book = stream
            .query_row("SELECT title, MIN(year) FROM book")
            .await
            .unwrap();
        if let Some(row) = book.row {
            println!("The oldest book is {} from year {}", row.title, row[1]);
        }

        // Fetch a single value, using a bound parameter
        let year = stream
            .query_value([
                "SELECT MAX(year) FROM book WHERE author = ?",
                vec!["Jane Austen"],
            ])
            .await
            .unwrap();
        if let Some(value) = year.value {
            println!("Last book from Jane Austen was published in {}", value);
        }

        // Execute a statement that does not return any rows
        let res = stream
            .execute(["DELETE FROM book WHERE author = ?", vec!["J. K. Rowling"]])
            .await
            .unwrap();
        println!("{} books have been cancelled", res.rows_affected);
    */

    // When you are done, remember to close the client
    client.lock().await.close().await.unwrap();
}
