use rustwave::Server;

// #[tokio::test]
// async fn test_server() {
//     let mut server = Server::bind("127.0.0.1:8080");
//     server.on_connection(|mut peer| async move {
//         println!("New connection: {}", peer.get_socket_id());

//         peer.on("message", |data, mut emitter| async move {
//             println!("Server got message: {}", data);
//             emitter.emit("message", "Hello i am server is here".to_string()).await.unwrap();
//         });

//         peer.on("join", |data, mut emitter| async move {
//             println!("User joined: {}", data);
//             emitter.emit("join", "server joined".to_string()).await.unwrap();
//         });

//         peer.on("leave", |data, _| async move {
//             println!("User left: {}", data);
//         });

//         peer.emit("welcome", "Hello".to_string()).await.unwrap();

//         peer.emit("message", "yooyoo".to_string()).await.unwrap();
//     });

//     server.run().await.unwrap();
// }










// #[tokio::test]
// async fn test_server() {
//     let mut server = Server::bind("127.0.0.1:8080");
    
//     let sh = server.handle();
    
//     server.on_connection(|peer| async move {
//         let socket = peer.get_socket_id();
//         println!("New connection: {}", socket);

//         peer.on("message", move |data, _| async move {
//             println!("Client {} say: {}", socket, data);
//         });
//     });

//     tokio::spawn(async move {
//         let stdin = tokio::io::stdin();
//         let reader = tokio::io::BufReader::new(stdin);
//         let mut lines = tokio::io::AsyncBufReadExt::lines(reader);

//         while let Ok(Some(line)) = lines.next_line().await {
//             if line.is_empty() { continue; }
            
//             match sh.broadcast("message", line.clone()).await {
//                 Ok(_) => println!("Server bheja: {}", line),
//                 Err(e) => println!("Error: {}", e)
//             }
//         }
//     });

//     server.run().await.unwrap();
// }




#[tokio::test]
async fn test_server() {
    let mut server = Server::bind("127.0.0.1:8080");
    
    let sh = server.handle();
    
    server.on_connection(|peer| async move {
        let socket = peer.get_socket_id();
        println!("New connection: {}", socket);

        peer.join_room("general");

        peer.on("message", move |data, ctx| async move {
            println!("Client {} say: {}", ctx.get_socket_id(), data);
            
          
            ctx.broadcast("message", data)
                .room("general")
                .expect(ctx.get_socket_id())
                .send().await.unwrap();
        });

        peer.on("disconnect", move |_, _| async move {
            println!("Client {} disconnected", socket);
        });

        peer.emit("welcome", "Hello! You are in general room.".to_string()).await.unwrap();
    });

    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let reader = tokio::io::BufReader::new(stdin);
        let mut lines = tokio::io::AsyncBufReadExt::lines(reader);

        println!("Type karo — sabko jayega general room mein:");

        while let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() { continue; }
            
            match sh.room_broadcast("general", "message", line.clone()).await {
                Ok(_) => println!("Server bheja room mein: {}", line),
                Err(e) => println!("Error: {}", e)
            }
        }
    });

    server.run().await.unwrap();
}