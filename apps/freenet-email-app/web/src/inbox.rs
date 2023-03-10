use chacha20poly1305::{aead::Aead, KeyInit, XChaCha20Poly1305, XNonce};
use chrono::{DateTime, Utc};
use ed25519_dalek::ed25519::signature::SignerMut;
use freenet_email_inbox::{
    Inbox as StoredInbox, InboxSettings as StoredSettings, Message as StoredMessage, UpdateInbox,
};
use locutus_aft_interface::{Tier, TokenAssignment};
use locutus_stdlib::{
    client_api::ContractRequest,
    prelude::{
        ContractCode, ContractContainer, ContractKey, Parameters, RelatedContracts, State,
        UpdateData, WasmAPIVersion, WrappedContract,
    },
};
use serde::{Deserialize, Serialize};

use crate::WebApi;

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

#[derive(Serialize, Deserialize, Debug, Default)]
struct DecryptedMessage {
    title: String,
    content: String,
    from: String,
    to: Vec<String>,
    cc: Vec<String>,
    time: DateTime<Utc>,
}

/// Inbox state
pub(crate) struct InboxModel {
    messages: Vec<Message>,
    settings: InternalSettings,
    key: ContractKey,
}

impl InboxModel {
    fn new() -> Self {
        let cipher = XChaCha20Poly1305::new(&[0u8; 32].into());
        Self {
            messages: vec![],
            settings: InternalSettings {
                next_msg_id: 0,
                minimum_tier: Tier::Hour1,
                encryption_key: (cipher, [0u8; 24].into()),
                signing_keypair: (ed25519_dalek::Keypair::from_bytes(&[1; 64]).unwrap()),
            },
            key: ContractKey::from((
                Parameters::from([].as_slice()),
                ContractCode::from([].as_slice()),
            )),
        }
    }

    fn to_state(&self) -> Result<State<'static>, Box<dyn std::error::Error>> {
        let settings = self.settings.to_stored()?;
        let messages = self
            .messages
            .iter()
            .map(|m| m.to_stored(&self.settings.encryption_key))
            .collect::<Result<Vec<_>, _>>()?;
        let inbox = StoredInbox::new(&self.settings.signing_keypair, settings, messages);
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
        let data = self.to_state()?;
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

    pub(crate) async fn get_inbox(
        client: &mut WebApi,
        keypair: &ed25519_dalek::Keypair,
        key: ContractKey,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let request = ContractRequest::Get {
            key,
            fetch_contract: false,
        };
        client.send(request.into()).await?;
        todo!()
    }

    #[cfg(debug_assertions)]
    pub(crate) fn create_inbox() -> Self {
        InboxModel::new()
    }

    #[cfg(not(debug_assertions))]
    pub(crate) async fn create_inbox(
        client: &mut WebApi,
        keypair: ed25519_dalek::Keypair,
        code: WrappedContract,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let inbox = InboxModel::new();
        let request = ContractRequest::Put {
            contract: WasmAPIVersion::V1(code).into(),
            state: inbox.to_state()?.as_ref().into(),
            related_contracts: RelatedContracts::new(),
        };
        client.send(request.into()).await?;
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locutus_stdlib::prelude::ContractInstanceId;

    use super::*;

    fn test_assignment() -> TokenAssignment {
        TokenAssignment {
            tier: Tier::Day1,
            time_slot: Default::default(),
            assignee: ed25519_dalek::PublicKey::from_bytes(&[0; 32]).unwrap(),
            signature: ed25519_dalek::Signature::from([1; 64]),
            assignment_hash: [0; 32],
            token_record: ContractInstanceId::from_str(
                "7MxRGrYiBBK2rHCVpP25SxqBLco2h4zpb2szsTS7XXgg",
            )
            .unwrap(),
        }
    }

    #[test]
    fn remove_msg() {
        let mut inbox = InboxModel::new();
        for id in 0..10000 {
            inbox.messages.push(Message {
                id,
                content: DecryptedMessage::default(),
                token_assignment: test_assignment(),
            });
        }
        eprintln!("started {}", chrono::Utc::now());
        let t0 = std::time::Instant::now();
        for id in 2500..7500 {
            inbox.remove_received_message(id);
        }
        eprintln!("{}ms", t0.elapsed().as_millis());
    }
}
