use tokio::sync::mpsc::Sender;

use crate::{error::Result, utils::send_data::EventAndData};

#[derive(Clone)]
pub struct PeerEmitter {
    sender: Sender<String>,
}

impl PeerEmitter {
    pub fn new(sender: Sender<String>) -> Self {
        Self { sender }
    }

    pub async fn emit(&mut self, event: &str, data: String) -> Result<()> {
        let json = serde_json::to_string(&EventAndData {
            event: event.to_string(),
            data,
        })?;

        match self.sender.send(json).await {
            Ok(_) => Ok(()),
            Err(_) => Err(crate::error::RustWaveError::Internal(
                "Message sendingg error".to_string(),
            )),
        }
    }
}
