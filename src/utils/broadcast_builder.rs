use std::sync::Arc;

use crate::error::Result;
use serde::Serialize;
use uuid::Uuid;

use crate::server::server_handler::ServerStates;

pub struct BroadcastBuilder<T: Serialize> {
    event: String,
    data: T,
    expect: Option<Uuid>,
    room: Option<String>,
    server: Arc<ServerStates>,
}

impl<T: Serialize> BroadcastBuilder<T> {
    pub(crate) fn new(event: String, data: T, server: Arc<ServerStates>) -> Self {
        Self {
            event,
            data,
            expect: None,
            room: None,
            server,
        }
    }

    pub fn expect(mut self, socket_id: Uuid) -> Self {
        self.expect = Some(socket_id);
        self    
    }

    pub fn room(mut self, room: &str) -> Self {
        self.room = Some(room.to_string());
        self
    }

    pub async fn send(&self) -> Result<()> {
        match &self.room {
            Some(v) => {
                if let Some(id) = self.expect {
                    self.server
                        .room_broadcast_except(&v, &id, &self.event, &self.data)
                        .await?;
                    Ok(())
                } else {
                    self.server
                        .room_broadcast(&v, &self.event, &self.data)
                        .await?;
                    Ok(())
                }
            }
            None => {
                self.server.broadcast(&self.event, &self.data).await?;
                Ok(())
            }
        }
    }
}
