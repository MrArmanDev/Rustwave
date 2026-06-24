use serde::{Deserialize, Serialize};


#[derive(Debug,  Serialize, Deserialize)]
pub struct EventAndData {
    pub event: String,
    pub data: String,
}