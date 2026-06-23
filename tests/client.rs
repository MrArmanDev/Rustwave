use rustwave::Client;


#[tokio::test]
async fn test() {
    let mut stream = Client::connect("ws://127.0.0.1:8080").await.unwrap();

    stream.send("Hello").await.unwrap();

    let mess = stream.read().await.unwrap();

    println!("Server: {}", mess);

    stream.send("ok").await.unwrap();


}
