use rustwave::Server;

#[tokio::test]
async fn test() {
    let mut server = Server::bind("127.0.0.1:8080");
    server.on_connection(|mut ws| async move {
        println!("New connection ");
        let mess = format!("Hello {}", ws.get_socket_id());
        ws.send(&mess).await.unwrap();

        let mess = ws.read().await.unwrap();
        println!("Client: {}", mess);

        let mess = ws.read().await.unwrap();
        println!("Client: {}", mess);



    });
    server.run().await.unwrap();


}
