use thiserror::Error;


#[derive(Debug, Error)]
pub enum RustWaveError {

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Client not found: {0}")]
    ClientNotFound(String),

    #[error("Connection closed")]
    ConnectionClosed,


    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WebSocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),
}


pub type Result<T> = std::result::Result<T, RustWaveError>;