use std::sync::Arc;

use serde::Serialize;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::{
    error::Result,
    server::server_handler::ServerStates,
    utils::{broadcast_builder::BroadcastBuilder, send_data::Response},
};

#[derive(Clone)]
pub struct PeerContext {
    socket_id: Uuid,
    sender: Sender<Message>,
    server_states: Arc<ServerStates>,
}

impl PeerContext {
    pub fn new(socket_id: Uuid, sender: Sender<Message>, server_states: Arc<ServerStates>) -> Self {
        Self {
            socket_id,
            sender,
            server_states,
        }
    }

    pub fn get_socket_id(&self) -> Uuid {
        self.socket_id
    }

    pub async fn emit<T: Serialize>(&self, event: &str, data: T) -> Result<()> {
        let json = serde_json::to_string(&Response {
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

    pub fn join_room(&self, room: &str) {
        self.server_states.insert_room(room, self.socket_id);
    }

    pub fn leave_room(&self, room: &str) {
        self.server_states.remove_room(room, &self.socket_id);
    }

    pub fn broadcast<T: Serialize>(&self, event: &str, data: T) -> BroadcastBuilder<T> {
        BroadcastBuilder::new(event.to_string(), data, self.server_states.clone())
    }

    pub async fn broadcast_to<T: Serialize>(&self, room: &str, event: &str, data: T) -> Result<()> {
        self.server_states.room_broadcast(room, event, data).await?;
        Ok(())
    }

    pub async fn broadcast_to_expect<T: Serialize>(
        &self,
        room: &str,
        socket_id: Uuid,
        event: &str,
        data: T,
    ) -> Result<()> {
        self.server_states
            .room_broadcast_except(room, &socket_id, event, data)
            .await?;
        Ok(())
    }
}
