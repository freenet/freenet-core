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
