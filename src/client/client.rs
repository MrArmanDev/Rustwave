use std::{pin::Pin, sync::Arc};

use dashmap::DashMap;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{error::Result, utils::send_data::EventAndData};

pub struct Client {
    sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    event: Arc<
        DashMap<
            String,
            Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
        >,
    >,
}

impl Client {
    pub async fn connect(url: &str) -> Result<Self> {
        let (stream, _) = connect_async(url).await?;
        let (write, read) = stream.split();

        Ok(Self {
            sender: write,
            reader: read,
            event: Arc::new(DashMap::new()),
        })
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

        match self.sender.send(Message::Text(json.into())).await {
            Ok(_) => Ok(()),
            Err(_) => Err(crate::error::RustWaveError::Internal(
                "Message sendingg error".to_string(),
            )),
        }
    }

    pub async fn listen(&mut self) -> Result<()> {
    let event = self.event.clone(); 

    loop {
        tokio::select! {
            mass = self.reader.next() => {
                match mass {
                    Some(Ok(Message::Text(text))) => {
                     
                        let parsed: EventAndData = serde_json::from_str(&text).unwrap();
                        
                        
                        if let Some(handler) = event.get(&parsed.event) {
                            handler(parsed.data).await;
                        }
                    }
                    _ => break 
                }
            }
        }
    }

    Ok(())
}
}
