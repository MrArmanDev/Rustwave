use crate::error::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use uuid::Uuid;

pub struct Peer {
    socket_id: Uuid,
    stream: WebSocketStream<TcpStream>,
}

impl Peer {
    pub fn new(stream: WebSocketStream<TcpStream>) -> Self {
        Self {
            socket_id: Uuid::new_v4(),
            stream,
        }
    }

    pub fn get_socket_id(&self) -> Uuid {
        self.socket_id
    }

    pub async fn send(&mut self, mess: &str) -> Result<()> {
        self.stream.send(Message::Text(mess.into())).await?;
        Ok(())
    }

    pub async fn send_json<T: Serialize>(&mut self, data: &T) -> Result<()> {
        let json = serde_json::to_string(data)?;
        self.stream.send(Message::Text(json.into())).await?;
        Ok(())
    }

    pub async fn read(&mut self) -> Result<String> {
        match self.stream.next().await {
            Some(Ok(Message::Text(mess))) => Ok(mess.to_string()),
            _ => Ok("Error reading message".to_string()),
        }
    }
}
