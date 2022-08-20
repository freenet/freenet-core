use locutus_stdlib::blake2::{Blake2b512, Digest};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub author: String,
    // date: DateTime<Utc>,
    pub title: String,
    pub content: String,
    #[serde(default = "Message::modded")]
    pub mod_msg: bool,
    pub signature: Option<ed25519_dalek::Signature>,
}

impl Message {
    pub fn hash(&self) -> [u8; 64] {
        let mut hasher = Blake2b512::new();
        hasher.update(self.author.as_bytes());
        hasher.update(self.title.as_bytes());
        hasher.update(self.content.as_bytes());
        let hash_val = hasher.finalize();
        let mut key = [0; 64];
        key.copy_from_slice(&hash_val[..]);
        key
    }

    pub fn modded() -> bool {
        false
    }
}

#[test]
fn test() -> Result<(), Box<dyn std::error::Error>> {
    use locutus_stdlib::prelude::StateDelta;
    let delta = StateDelta::from(vec![
        123u8, 10, 9, 9, 9, 34, 97, 117, 116, 104, 111, 114, 34, 58, 32, 34, 73, 68, 71, 34, 44,
        10, 9, 9, 9, 34, 100, 97, 116, 101, 34, 58, 32, 34, 50, 48, 50, 50, 45, 48, 53, 45, 49, 48,
        84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 34, 44, 10, 9, 9, 9, 34, 116, 105, 116, 108, 101,
        34, 58, 32, 34, 76, 111, 114, 101, 32, 105, 112, 115, 117, 109, 34, 44, 10, 9, 9, 9, 34,
        99, 111, 110, 116, 101, 110, 116, 34, 58, 32, 34, 76, 111, 114, 101, 109, 32, 105, 112,
        115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44,
        32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115,
        99, 105, 110, 103, 32, 101, 108, 105, 116, 44, 32, 115, 101, 100, 32, 100, 111, 32, 101,
        105, 117, 115, 109, 111, 100, 32, 116, 101, 109, 112, 111, 114, 32, 105, 110, 99, 105, 100,
        105, 100, 117, 110, 116, 32, 117, 116, 32, 108, 97, 98, 111, 114, 101, 32, 101, 116, 32,
        100, 111, 108, 111, 114, 101, 32, 109, 97, 103, 110, 97, 32, 97, 108, 105, 113, 117, 97,
        46, 34, 10, 9, 9, 125,
    ]);

    let _json: serde_json::Value = serde_json::from_slice(delta.as_ref())?;
    Ok(())
}
