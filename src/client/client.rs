use std::{pin::Pin, sync::Arc};

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use crate::{error::Result, utils::send_data::EventAndData};

pub struct Client {
    sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    token: CancellationToken,
    events: Arc<
        DashMap<
            String,
            Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
        >,
    >,
}

impl Client {
    pub async fn connect(url: &str) -> Result<Self> {
        let (stream, _) = connect_async(url).await?;
        let (write, mut read) = stream.split();

        let events: Arc<
            DashMap<
                String,
                Arc<
                    dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
                        + Send
                        + Sync
                        + 'static,
                >,
            >,
        > = Arc::new(DashMap::new());
        let read_event = events.clone();

        let token = CancellationToken::new();
        let read_token = token.clone();
        let wait_token = token.clone();

        tokio::spawn(async move {
            let read_event = read_event.clone();
            loop {
                tokio::select! {
                    mass = read.next() => {
                        match mass {
                            Some(Ok(Message::Text(text))) => {

                                let parsed: EventAndData = serde_json::from_str(&text).unwrap();


                                if let Some(handler) = read_event.get(&parsed.event) {
                                    handler(parsed.data).await;
                                }
                            }
                            Some(Ok(_)) => {}
                            None | Some(Err(_)) => {
                                read_token.cancel();
                            }
                        }
                    }

                    _ = read_token.cancelled() => break
                }
            }
        });

        Ok(Self {
            sender: write,
            token: wait_token,
            events: events,
        })
    }

    pub fn on<F, Fut>(&mut self, event: &str, callback: F)
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.events
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

    pub async fn wait(&self) {
        self.token.cancelled().await;
    }
}
