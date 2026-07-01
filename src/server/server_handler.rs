use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::error::{Result, RustWaveError};

use crate::utils::send_data::Response;

pub struct ServerStates {
    clients: Arc<DashMap<Uuid, Sender<Message>>>,
    rooms: Arc<DashMap<String, DashSet<Uuid>>>,
}

impl ServerStates {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(DashMap::new()),
            rooms: Arc::new(DashMap::new()),
        }
    }

    pub(crate) fn insert_client(&self, id: Uuid, sender: Sender<Message>) {
        self.clients.insert(id, sender);
    }

    pub(crate) fn remove_client(&self, id: &Uuid) {
        self.clients.remove(id);
    }

    pub(crate) fn insert_room(&self, room: &str, id: Uuid) {
        self.rooms
            .entry(room.to_string())
            .or_insert_with(DashSet::new)
            .insert(id);
    }

    pub(crate) fn remove_room(&self, room: &str, id: &Uuid) {
        if let Some(room) = self.rooms.get_mut(room) {
            room.remove(id);
        }
    }

    pub async fn room_broadcast<T: Serialize>(
        &self,
        room: &str,
        event: &str,
        data: T,
    ) -> Result<()> {
        let json = serde_json::to_string(&Response {
            event: event.to_string(),
            data,
        })?;

        let mess = Message::Text(json.into());

        for con in self.rooms.get(room).iter() {
            let sender = con.value();

            for client in sender.iter() {
                let tx = self.clients.get(&client);

                if let Some(tx) = tx {
                    let _ = tx.value().send(mess.clone()).await;
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn room_broadcast_except<T: Serialize>(
        &self,
        room: &str,
        socket_id: &Uuid,
        event: &str,
        data: T,
    ) -> Result<()> {
        let json = serde_json::to_string(&Response {
            event: event.to_string(),
            data,
        })?;

        let mess = Message::Text(json.into());

        if let Some(con) = self.rooms.get(room) {
            let sender = con.value();

            for client in sender.iter() {
                if client.key() == socket_id {
                    continue;
                }
                let tx = self.clients.get(client.key());

                if let Some(tx) = tx {
                    let _ = tx.value().send(mess.clone()).await;
                }
            }
        }

        Ok(())
    }

    pub async fn broadcast<T: Serialize>(&self, event: &str, data: T) -> Result<()> {
        let json = serde_json::to_string(&Response {
            event: event.to_string(),
            data,
        })?;

        let mess = Message::Text(json.into());

        for con in self.clients.iter() {
            let sender = con.value();
            let _ = sender.send(mess.clone()).await;
        }

        Ok(())
    }

    pub async fn emit_to<T: Serialize>(
        &self,
        socket_id: &Uuid,
        event: &str,
        message: T,
    ) -> Result<()> {
        let client = match self.clients.get(socket_id) {
            Some(client) => client,
            None => return Err(RustWaveError::ClientNotFound(socket_id.to_string())),
        };

        let json = serde_json::to_string(&Response {
            event: event.to_string(),
            data: message,
        })?;

        match client.value().send(Message::Text(json.into())).await {
            Ok(_) => Ok(()),
            Err(_) => Err(RustWaveError::ConnectionClosed),
        }
    }

    pub fn is_connected(&self, socket_id: &Uuid) -> bool {
        self.clients.contains_key(socket_id)
    }

    pub fn get_client_count(&self) -> usize {
        self.clients.len()
    }

    pub async fn disconnect(&self, socket_id: &Uuid) -> Result<()> {
        match self.clients.get(socket_id) {
            Some(sender) => {
                let _ = sender.send(Message::Close(None)).await;
                Ok(())
            }
            None => Err(RustWaveError::ClientNotFound(socket_id.to_string())),
        }
    }

    pub async fn disconnect_all(&self) -> Result<()> {
        let senders = self
            .clients
            .iter()
            .map(|v| v.value().clone())
            .collect::<Vec<Sender<Message>>>();

        for sender in senders {
            let _ = sender.send(Message::Close(None)).await;
        }

        Ok(())
    }
}
