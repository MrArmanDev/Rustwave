use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::error::Result;

use crate::utils::send_data::EventAndData;

pub struct ServerHandler {
    client: Arc<DashMap<Uuid, Sender<Message>>>,
}

impl ServerHandler {
    pub fn new(client: Arc<DashMap<Uuid, Sender<Message>>>) -> Self {
        Self { client }
    }

    pub async fn broadcast(&self, event: &str, data: String) -> Result<()> {
        let json = serde_json::to_string(&EventAndData {
            event: event.to_string(),
            data,
        })?;

        let mess = Message::Text(json.into());

        for con in self.client.iter() {
            let sender = con.value();
            let _ = sender.send(mess.clone()).await;
        }

        Ok(())
    }
}
