use tokio_tungstenite::tungstenite::protocol::Message;

pub struct DepthUpdate {
    pub first_update_id: u64,
    pub last_update_id: u64,
    pub value: serde_json::Value,
}

impl DepthUpdate {
    pub fn from_message(message: Message) -> Option<DepthUpdate> {
        let Message::Text(text) = message else {
            return None;
        };

        let json = serde_json::from_str::<serde_json::Value>(&text).ok()?;
        let last_update_id = json.get("u")?.as_u64()?;
        let first_update_id = json.get("U")?.as_u64()?;

        Some(DepthUpdate {
            first_update_id,
            last_update_id,
            value: json,
        })
    }
}
