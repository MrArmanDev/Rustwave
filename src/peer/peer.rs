use std::{pin::Pin, sync::Arc};

use dashmap::DashMap;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::peer::peer_emitter::PeerEmitter;
use crate::utils::send_data::EventAndData;

use crate::error::Result;

pub struct Peer {
    socket_id: Uuid,
    sender: Sender<Message>,
    event: Arc<
        DashMap<
            String,
            Arc<dyn Fn(String, PeerEmitter) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
        >,
    >,
}

impl Peer {
    pub fn new(sender: Sender<Message>) -> Self {
        Self {
            socket_id: Uuid::new_v4(),
            sender,
            event: Arc::new(DashMap::new()),
        }
    }

    pub fn get_socket_id(&self) -> Uuid {
        self.socket_id
    }

    pub(crate) fn get_event(
        &self,
    ) -> Arc<
        DashMap<
            String,
            Arc<dyn Fn(String, PeerEmitter) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
        >,
    > {
        self.event.clone()
    }

    pub fn on<F, Fut>(&mut self, event: &str, callback: F)
    where
        F: Fn(String, PeerEmitter) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.event
            .insert(event.to_string(), Arc::new(move |v, y| Box::pin(callback(v, y))));
    }

    pub async fn emit(&mut self, event: &str, data: String) -> Result<()> {
        let json = serde_json::to_string(&EventAndData {
            event: event.to_string(),
            data,
        })?;

        match self.sender.send(Message::Text(json.into())).await {
            Ok(_) => Ok(()),
            Err(_) => Err(crate::error::RustWaveError::Internal(
                "Message sendingg error".to_string(),
            )),
        }
    }
    
}
