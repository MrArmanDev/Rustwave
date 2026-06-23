use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::error::Result;

pub struct Client {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Client {
    pub async fn connect(url: &str) -> Result<Self> {
        let (stream, _) = connect_async(url).await?;
        Ok(Self { stream })
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
