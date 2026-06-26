use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;

use crate::{error::Result, utils::send_data::EventAndData};

#[derive(Clone)]
pub struct PeerEmitter {
    sender: Sender<Message>,
}

impl PeerEmitter {
    pub fn new(sender: Sender<Message>) -> Self {
        Self { sender }
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
