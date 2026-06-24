use rustwave::Client;

#[tokio::test]
async fn test_client() {
    let mut client = Client::connect("ws://127.0.0.1:8080").await.unwrap();

    client.on("message", |data| async move {
        println!("Client got message: {}", data);
    });

    client.on("welcome", |data| async move {
        println!("Server said: {}", data);
    });


    client.emit("join", "Rahul".to_string()).await.unwrap();
    client.emit("message", "Hello Server!".to_string()).await.unwrap();
    client.emit("message", "Kya haal hai?".to_string()).await.unwrap();
    client.emit("leave", "Rahul".to_string()).await.unwrap();

    client.listen().await.unwrap();
}