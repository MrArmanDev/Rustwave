use std::sync::Arc;
use std::{future::Future, pin::Pin};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

use crate::Peer;
use crate::error::Result;

pub struct Server {
    add: String,
    handler: Option<Arc<dyn Fn(Peer) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>>,
}

impl Server {
    pub fn bind(adr: &str) -> Self {
        Self {
            add: adr.to_string(),
            handler: None,
        }
    }

    pub fn on_connection<T, F>(&mut self, handler: T)
    where
        T: Fn(Peer) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.handler = Some(Arc::new(move |client| Box::pin(handler(client))));
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.add).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let handler = self.handler.clone();

            tokio::spawn(async move {
                match accept_async(stream).await {
                    Ok(ws) => {
                        let client = Peer::new(ws);
                        if let Some(handler) = handler {
                            handler(client).await;
                        }
                    }
                    Err(_) => {}
                }
            });
        }
    }
}
