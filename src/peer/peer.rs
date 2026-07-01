use std::{pin::Pin, sync::Arc};

use dashmap::{DashMap, DashSet};
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::peer::peer_conext::PeerContext;
use crate::server::server_handler::ServerStates;
use crate::utils::send_data::Response;

use crate::error::Result;

pub struct Peer {
    socket_id: Uuid,
    sender: Sender<Message>,
    event: Arc<
        DashMap<
            String,
            Arc<
                dyn Fn(String, PeerContext) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        >,
    >,

    server_states: Arc<ServerStates>,
    rooms: Arc<DashSet<String>>
}

impl Peer {
    pub fn new(sender: Sender<Message>, server_states: Arc<ServerStates>) -> Self {
        Self {
            socket_id: Uuid::new_v4(),
            sender,
            event: Arc::new(DashMap::new()),
            server_states,
            rooms: Arc::new(DashSet::new())
        }
    }

    pub fn get_socket_id(&self) -> Uuid {
        self.socket_id
    }

    pub fn get_rooms(&self) -> Arc<DashSet<String>> {
        self.rooms.clone()
    }

    pub(crate) fn get_event(
        &self,
    ) -> Arc<
        DashMap<
            String,
            Arc<
                dyn Fn(String, PeerContext) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        >,
    > {
        self.event.clone()
    }

    pub fn on<F, Fut>(&self, event: &str, callback: F)
    where
        F: Fn(String, PeerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.event.insert(
            event.to_string(),
            Arc::new(move |v, y| Box::pin(callback(v, y))),
        );
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
        self.rooms.insert(room.to_string());
    }

    pub fn leave_room(&self, room: &str) {
        self.server_states.remove_room(room, &self.socket_id);
        self.rooms.remove(&room.to_string());
    }


}
