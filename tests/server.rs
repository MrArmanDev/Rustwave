use rustwave::Server;

#[tokio::test]
async fn test_server() {
    let mut server = Server::bind("127.0.0.1:8080");
    server.on_connection(|mut peer| async move {
        println!("New connection: {}", peer.get_socket_id());

        peer.on("message", |data, mut emitter| async move {
            println!("Server got message: {}", data);
            emitter.emit("message", "Hello i am server is here".to_string()).await.unwrap();
        });

        peer.on("join", |data, mut emitter| async move {
            println!("User joined: {}", data);
            emitter.emit("join", "server joined".to_string()).await.unwrap();
        });

        peer.on("leave", |data, _| async move {
            println!("User left: {}", data);
        });

        peer.emit("welcome", "Hello".to_string()).await.unwrap();

        peer.emit("message", "yooyoo".to_string()).await.unwrap();
    });

    server.run().await.unwrap();
}
