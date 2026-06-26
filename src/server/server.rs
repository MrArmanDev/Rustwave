use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::{future::Future, pin::Pin};
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::Peer;
use crate::error::Result;
use crate::peer::peer_emitter::PeerEmitter;
use crate::server::server_handler::ServerHandler;
use crate::utils::send_data::EventAndData;

pub struct Server {
    add: String,
    handler: Option<Arc<dyn Fn(Peer) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>>,
    client: Arc<DashMap<Uuid, Sender<Message>>>,
}

impl Server {
    pub fn bind(adr: &str) -> Self {
        Self {
            add: adr.to_string(),
            handler: None,
            client: Arc::new(DashMap::new()),
        }
    }

    pub fn on_connection<T, F>(&mut self, handler: T)
    where
        T: Fn(Peer) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.handler = Some(Arc::new(move |client| Box::pin(handler(client))));
    }

    pub fn handle(&self) -> ServerHandler {
        ServerHandler::new(self.client.clone())
    }



    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.add).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let handler = self.handler.clone();

            let registry = self.client.clone();

            tokio::spawn(async move {
                match accept_async(stream).await {
                    Ok(ws) => {
                        let (mut write, mut read) = ws.split();

                        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(100);
                        let tx_sender = tx.clone();

                        let client = Peer::new(tx.clone());
                        let tx_emitter = PeerEmitter::new(tx.clone());

                        let socket_id = client.get_socket_id();

                        registry.insert(socket_id, tx.clone());

                        let events = client.get_event();

                        let token = CancellationToken::new();
                        let read_token = token.clone();
                        let write_token = token.clone();

                        let read_task = tokio::spawn(async move {
                            let emitter = tx_emitter;
                            let sender = tx_sender;

                            loop {
                                tokio::select! {
                                            mass = read.next() => {
                                                match mass {
                                                    Some(m) => {
                                                        match m {
                                                            Ok(message) => {
                                                                match message {
                                                                    Message::Text(text) => {
                                                                        let parsed = match serde_json::from_str::<EventAndData>(&text) {
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
                                                                            Err(_) => continue
                                                                        }
                                                                    }

                                                                    Message::Close(frame) => {
                                                                        match sender.send(Message::Close(frame)).await {
                                                                            Ok(_) => {}
                                                                            Err(_) => continue
                                                                        }
                                                                    }

                                                                    _ => {}
                                                                }
                                                            }

                                                            Err(_) => {
                                                                registry.remove(&socket_id);
                                                                read_token.cancel();
                                                                break
                                                            }
                                                        }
                                                    }
                                                    None => {
                                                        registry.remove(&socket_id);
                                                        read_token.cancel();
                                                        break
                                                    }
                                                }
                                            }

                                            _ = read_token.cancelled() => break
                                        }
                            }
                        });

                        let write_task = tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    mass = rx.recv() => {
                                        match mass {
                                            Some(m) => {
                                                match write.send(m).await {
                                                    Ok(_v) => {}
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

                        let _ = tokio::join!(read_task, write_task);
                    }
                    Err(_) => {}
                }
            });
        }
    }
}
