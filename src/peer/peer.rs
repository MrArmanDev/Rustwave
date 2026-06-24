use std::{pin::Pin, sync::Arc};

use dashmap::DashMap;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::utils::send_data::EventAndData;

use crate::error::Result;

pub struct Peer {
    socket_id: Uuid,
    sender: Sender<String>,
    event: Arc<
        DashMap<
            String,
            Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
        >,
    >,
}

impl Peer {
    pub fn new(sender: Sender<String>) -> Self {
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
            Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
        >,
    > {
        self.event.clone()
    }

    pub fn on<F, Fut>(&mut self, event: &str, callback: F)
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.event
            .insert(event.to_string(), Arc::new(move |v| Box::pin(callback(v))));
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
