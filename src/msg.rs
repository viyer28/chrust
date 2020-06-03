use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct RcvdMessage {
    pub r#type: String,
    pub key: Option<String>,
    pub value: Option<String>,
    pub id: Option<i32>,
    pub destination: Option<String>,
}

#[derive(Serialize)]
pub struct HelloResponse {
    r#type: String,
    source: String,
}

impl HelloResponse {
    pub fn new(source: &str) -> HelloResponse {
        HelloResponse {
            r#type: String::from("helloResponse"),
            source: String::from(source),
        }
    }
}

#[derive(Serialize)]
pub struct SetResponse {
    r#type: String,
    id: i32,
    key: String,
    value: String,
}

impl SetResponse {
    pub fn new(id: i32, key: &str, value: &str) -> SetResponse {
        SetResponse {
            r#type: String::from("setResponse"),
            id,
            key: String::from(key),
            value: String::from(value),
        }
    }
}

#[derive(Serialize)]
pub struct GetSuccessResponse {
    r#type: String,
    id: i32,
    key: String,
    value: String,
}

impl GetSuccessResponse {
    pub fn new(id: i32, key: &str, value: &str) -> GetSuccessResponse {
        GetSuccessResponse {
            r#type: String::from("getResponse"),
            id,
            key: String::from(key),
            value: String::from(value),
        }
    }
}

#[derive(Serialize)]
pub struct GetFailResponse {
    r#type: String,
    id: i32,
    error: String,
}

impl GetFailResponse {
    pub fn new(id: i32, key: &str) -> GetFailResponse {
        GetFailResponse {
            r#type: String::from("getResponse"),
            id,
            error: String::from("No such key: ") + key,
        }
    }
}
