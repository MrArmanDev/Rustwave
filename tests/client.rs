use std::sync::Arc;

use rustwave::Client;

// #[tokio::test]
// async fn test_client() {
//     let mut client = Client::connect("ws://127.0.0.1:8080").await.unwrap();

//     client.on("message", |data| async move {
//         println!("Client got message: {}", data);
//     });

//     client.on("welcome", |data| async move {
//         println!("Server said: {}", data);
//     });

//     client.on("join", |data| async move {
//         println!("Server joined: {}", data);
//     });


//     client.emit("join", "Rahul".to_string()).await.unwrap();
//     client.emit("message", "Hello Server!".to_string()).await.unwrap();
//     client.emit("message", "Kya haal hai?".to_string()).await.unwrap();
//     client.emit("leave", "Rahul".to_string()).await.unwrap();

//     client.wait().await
// }




// #[tokio::test]
// async fn test_client() {
//     let client = Client::connect("ws://127.0.0.1:8080").await.unwrap();
 

//     client.on("message", |data| async move {
//         println!("Server say: {}", data);
//     });

//     let client = Arc::new(client);
//     let client_clone = client.clone();

   
//     tokio::spawn(async move {
//         let stdin = tokio::io::stdin();
//         let reader = tokio::io::BufReader::new(stdin);
//         let mut lines = tokio::io::AsyncBufReadExt::lines(reader);

//         while let Ok(Some(line)) = lines.next_line().await {
//             if line.is_empty() { continue; }
            
//             match client_clone.emit("message", line.clone()).await {
//                 Ok(_) => println!("Tune bheja: {}", line),
//                 Err(e) => println!("Error: {}", e)
//             }
//         }
//     });

//     client.wait().await;
// }


#[tokio::test]
async fn test_client() {
    let client = Client::connect("ws://127.0.0.1:8080").await.unwrap();

    client.on("welcome", |data| async move {
        println!("Server: {}", data);
    });

    client.on("message", |data| async move {
        println!("Room message: {}", data);
    });

    client.on("disconnect", |_| async move {
        println!("Server se disconnect ho gaya!");
    });

    let client = Arc::new(client);
    let client_clone = client.clone();

    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let reader = tokio::io::BufReader::new(stdin);
        let mut lines = tokio::io::AsyncBufReadExt::lines(reader);

        println!("Type karo — room mein jayega:");

        while let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() { continue; }
            
            match client_clone.emit("message", line.clone()).await {
                Ok(_) => println!("Tune bheja: {}", line),
                Err(e) => println!("Error: {}", e)
            }
        }
    });

    client.wait().await;
}