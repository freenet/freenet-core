use chacha20poly1305::{aead::Aead, XChaCha20Poly1305, XNonce};
use chrono::{DateTime, Utc};
use dioxus::prelude::*;
use ed25519_dalek::ed25519::signature::SignerMut;
use freenet_email_inbox::{
    Inbox as StoredInbox, InboxSettings as StoredSettings, Message as StoredMessage, UpdateInbox,
};
use locutus_aft_interface::{Tier, TokenAssignment};
use locutus_stdlib::{
    client_api::ContractRequest,
    prelude::{ContractKey, State, UpdateData},
};
use serde::{Deserialize, Serialize};

#[cfg(target_family = "unix")]
use locutus_stdlib::client_api::WebApi as OriginalWebApi;
#[cfg(target_family = "unix")]
type WebApi = OriginalWebApi;

pub fn main() {
    #[cfg(not(target_family = "wasm"))]
    dioxus_desktop::launch(app);

    #[cfg(target_family = "wasm")]
    dioxus_web::launch(app);
}

fn app(cx: Scope) -> Element {
    cx.render(rsx! {
        div { "hello, wasm!" }
    })
}

struct InternalSettings {
    /// This id is used for internal handling of the inbox and is not persistent
    /// or unique across sessions.
    next_msg_id: usize,
    minimum_tier: Tier,
    /// Used with the nonce for encrypting private data only knowledgeable to the user,
    encryption_key: (XChaCha20Poly1305, XNonce),
    /// Used for signing modifications to the state that are to be persisted.
    /// The public key must be the same as the one used for the inbox contract.
    signing_keypair: ed25519_dalek::Keypair,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredDecryptedSettings {}

impl InternalSettings {
    fn from_stored(
        stored_settings: StoredSettings,
        next_id: usize,
        encryption_key: (XChaCha20Poly1305, XNonce),
        keypair: ed25519_dalek::Keypair,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // let settings = cipher.decrypt(&nonce, stored_settings.private.as_ref())?;
        // let settings: StoredDecryptedSettings = serde_json::from_slice(&settings)?;
        Ok(Self {
            next_msg_id: next_id,
            encryption_key,
            signing_keypair: keypair,
            minimum_tier: stored_settings.minimum_tier,
        })
    }

    fn to_stored(&self) -> Result<StoredSettings, Box<dyn std::error::Error>> {
        // let private = serde_json::to_vec(&StoredDecryptedSettings {})?;
        Ok(StoredSettings {
            minimum_tier: self.minimum_tier,
            private: vec![],
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    id: usize,
    content: DecryptedMessage,
    token_assignment: TokenAssignment,
}

impl Message {
    fn to_stored(
        &self,
        (cipher, nonce): &(XChaCha20Poly1305, XNonce),
    ) -> Result<StoredMessage, Box<dyn std::error::Error>> {
        let encrypted_content = serde_json::to_vec(&self.content)?;
        let content = cipher.encrypt(nonce, encrypted_content.as_ref()).unwrap();
        Ok::<_, Box<dyn std::error::Error>>(StoredMessage {
            content,
            token_assignment: self.token_assignment.clone(),
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct DecryptedMessage {
    title: String,
    content: String,
    from: String,
    to: Vec<String>,
    cc: Vec<String>,
    time: DateTime<Utc>,
}

/// Inbox state
struct Inbox {
    messages: Vec<Message>,
    settings: InternalSettings,
    key: ContractKey,
}

impl Inbox {
    fn into_state(&self) -> Result<State<'static>, Box<dyn std::error::Error>> {
        let settings = self.settings.to_stored()?;
        let messages = self
            .messages
            .iter()
            .map(|m| m.to_stored(&self.settings.encryption_key))
            .collect::<Result<Vec<_>, _>>()?;
        let inbox = StoredInbox {
            settings,
            last_update: Utc::now(),
            messages,
        };
        let serialized = serde_json::to_vec(&inbox)?;
        Ok(serialized.into())
    }

    fn from_state(
        (cipher, nonce): (XChaCha20Poly1305, XNonce),
        keypair: ed25519_dalek::Keypair,
        state: StoredInbox,
        key: ContractKey,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let messages = state
            .messages
            .iter()
            .enumerate()
            .map(|(id, msg)| {
                let decrypted_content = cipher.decrypt(&nonce, msg.content.as_ref())?;
                let content: DecryptedMessage = serde_json::from_slice(&decrypted_content)?;
                Ok(Message {
                    id,
                    content,
                    token_assignment: msg.token_assignment.clone(),
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        Ok(Self {
            settings: InternalSettings::from_stored(
                state.settings,
                messages.len(),
                (cipher, nonce),
                keypair,
            )?,
            key,
            messages,
        })
    }

    /// This only affects in-memory messages, changes are not persisted.
    fn add_received_message(
        &mut self,
        content: DecryptedMessage,
        token_assignment: TokenAssignment,
    ) {
        self.messages.push(Message {
            id: self.settings.next_msg_id,
            content,
            token_assignment,
        });
        self.settings.next_msg_id += 1;
    }

    /// This only affects in-memory messages, changes are not persisted.
    fn remove_received_message(&mut self, id: usize) {
        if let Ok(p) = self.messages.binary_search_by_key(&id, |a| a.id) {
            self.messages.remove(p);
        }
    }

    async fn store_all_messages(
        &self,
        client: &mut WebApi,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let data = self.into_state()?;
        let request = ContractRequest::Update {
            key: self.key.clone(),
            data: UpdateData::State(data),
        };
        client.send(request.into()).await?;
        Ok(())
    }

    async fn update_settings_at_store(
        &mut self,
        client: &mut WebApi,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let settings = self.settings.to_stored()?;
        let serialized = serde_json::to_vec(&settings)?;
        let signature = self.settings.signing_keypair.sign(&serialized);
        let delta = UpdateInbox::ModifySettings {
            signature,
            settings,
        };
        let request = ContractRequest::Update {
            key: self.key.clone(),
            data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
        };
        client.send(request.into()).await?;
        Ok(())
    }

    async fn remove_messages_from_store(
        &mut self,
        client: &mut WebApi,
        ids: &[usize],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut signed = Vec::with_capacity(ids.len() * 32);
        let mut ids = Vec::with_capacity(ids.len() * 32);
        for m in &self.messages {
            let h = &m.token_assignment.assignment_hash;
            signed.extend(h);
            ids.push(*h);
        }
        let signature = self.settings.signing_keypair.sign(&signed);
        let delta = UpdateInbox::RemoveMessages { signature, ids };
        let request = ContractRequest::Update {
            key: self.key.clone(),
            data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
        };
        client.send(request.into()).await?;
        Ok(())
    }

    async fn add_messages_to_store(
        &mut self,
        client: &mut WebApi,
        ids: &[usize],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut signed = Vec::with_capacity(ids.len() * 32);
        let mut messages = Vec::with_capacity(ids.len() * 32);
        for m in &self.messages {
            let h = &m.token_assignment.assignment_hash;
            signed.extend(h);
            messages.push(m.to_stored(&self.settings.encryption_key)?);
        }
        let signature = self.settings.signing_keypair.sign(&signed);
        let delta = UpdateInbox::AddMessages {
            signature,
            messages,
        };
        let request = ContractRequest::Update {
            key: self.key.clone(),
            data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
        };
        client.send(request.into()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn remove_msg() {
        // let mut inbox = Inbox::default();
        // for id in 0..100000 {
        //     inbox.messages.push(Message {
        //         id,
        //         ..Default::default()
        //     });
        // }
        // eprintln!("started");
        // let t0 = std::time::Instant::now();
        // for id in 25000..75000 {
        //     inbox.remove_message(id);
        // }
        // eprintln!("{}ms", t0.elapsed().as_millis());
    }
}
