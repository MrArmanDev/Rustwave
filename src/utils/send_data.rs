use serde::{Deserialize, Serialize};


#[derive(Debug,  Serialize, Deserialize)]
pub struct Response<T: Serialize>    {
    pub event: String,
    pub data: T,
}


#[derive(Debug,  Serialize, Deserialize)]
pub struct Request {
    pub event: String,
    pub data: String,
}