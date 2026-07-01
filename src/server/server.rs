use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::{future::Future, pin::Pin};
use tokio::net::TcpListener;

use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::Peer;
use crate::error::Result;
use crate::peer::peer_conext::PeerContext;
use crate::server::server_handler::ServerStates;
use crate::utils::send_data::Request;

pub struct Server {
    add: String,
    handler: Option<Arc<dyn Fn(Peer) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>>,
    server_states: Arc<ServerStates>,
}

impl Server {
    pub fn bind(adr: &str) -> Self {
        Self {
            add: adr.to_string(),
            handler: None,
            server_states: Arc::new(ServerStates::new()),
        }
    }

    pub fn on_connection<T, F>(&mut self, handler: T)
    where
        T: Fn(Peer) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.handler = Some(Arc::new(move |client| Box::pin(handler(client))));
    }

    pub fn handle(&self) -> Arc<ServerStates> {
        self.server_states.clone()
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.add).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let handler = self.handler.clone();

            let registry = self.server_states.clone();

            tokio::spawn(async move {
                match accept_async(stream).await {
                    Ok(ws) => {
                        let (mut write, mut read) = ws.split();

                        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(100);
                        let tx_sender = tx.clone();

                        let client = Peer::new(tx.clone(), registry.clone());
                        let client_rooms = client.get_rooms();

                        let context =
                            PeerContext::new(client.get_socket_id(), tx.clone(), registry.clone());

                        let socket_id = client.get_socket_id();

                        registry.insert_client(socket_id, tx.clone());

                        let events = client.get_event();

                        let token = CancellationToken::new();
                        let read_token = token.clone();
                        let write_token = token.clone();

                        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();

                        let read_task = tokio::spawn(async move {
                            let emitter = context;
                            let sender = tx_sender;
                            let rooms = client_rooms;

                            let _ = ready_rx.await;

                            loop {
                                tokio::select! {
                                    mass = read.next() => {
                                        match mass {
                                            Some(m) => {
                                                match m {
                                                    Ok(message) => {
                                                        match message {
                                                            Message::Text(text) => {
                                                                let parsed = match serde_json::from_str::<Request>(&text) {
                                                                    Ok(v) => v,
                                                                    Err(_) => continue
                                                                };

                                                                if let Some(handler) = events.get(&parsed.event) {
                                                                    handler(parsed.data, emitter.clone()).await;
                                                                }


                                                            }

                                                            Message::Ping(data) => {
                                                                match sender.send(Message::Pong(data)).await {
                                                                    Ok(_) => {}
                                                                    Err(_) => break
                                                                }
                                                            }

                                                            Message::Close(frame) => {
                                                                match sender.send(Message::Close(frame)).await {
                                                                    Ok(_) => break,
                                                                    Err(_) => break
                                                                }
                                                            }

                                                            _ => {}
                                                        }
                                                    }

                                                    Err(_) => break
                                                }
                                            }
                                            None => break
                                        }
                                    }

                                    _ = read_token.cancelled() => break
                                }
                            }

                            registry.remove_client(&socket_id);

                            for room in rooms.iter() {
                                registry.remove_room(&room, &socket_id);
                            }
                            
                            read_token.cancel();

                            if let Some(handler) = events.get("disconnect") {
                                handler("".to_string(), emitter.clone()).await
                            }
                        });

                        let write_task = tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    mass = rx.recv() => {
                                        match mass {
                                            Some(m) => {
                                                match write.send(m).await {
                                                    Ok(_) => {}
                                                    Err(_) => {
                                                        write_token.cancel();
                                                        break
                                                    }
                                                }
                                            }
                                            None => {
                                                write_token.cancel();
                                                break
                                            }
                                        }
                                    }

                                    _ = write_token.cancelled() => break
                                }
                            }
                        });

                        if let Some(handler) = handler {
                            handler(client).await;
                        }

                        let _ = ready_tx.send(());

                        let _ = tokio::join!(read_task, write_task);
                    }
                    Err(_) => {}
                }
            });
        }
    }
}
