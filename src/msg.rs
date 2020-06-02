use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ControlMessages {
    pub r#type: String,
    pub key: Option<String>,
    pub value: Option<String>,
    pub id: Option<String>,
    pub destination: Option<String>,
}

#[derive(Serialize)]
pub struct HeyBack {
    r#type: String,
    source: String,
}

impl HeyBack {
    pub fn new(source: &str) -> HeyBack {
        HeyBack{
            r#type: String::from("helloResponse"),
            source: String::from(source),
        }
    }
}