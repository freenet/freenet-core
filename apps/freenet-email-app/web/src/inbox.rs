use chrono::{DateTime, Utc};
use freenet_email_inbox::{
    Inbox as StoredInbox, InboxParams, InboxSettings as StoredSettings, Message as StoredMessage,
    UpdateInbox,
};
use locutus_aft_interface::{Tier, TokenAssignment};
use locutus_stdlib::{
    client_api::ContractRequest,
    prelude::{ContractCode, ContractKey, State, UpdateData},
};
use rand_chacha::rand_core::SeedableRng;
use rsa::{
    pkcs1v15::SigningKey, sha2::Sha256, signature::Signer, Pkcs1v15Encrypt, PublicKey,
    RsaPrivateKey,
};
use serde::{Deserialize, Serialize};

use crate::WebApi;

#[derive(Debug, Clone)]
struct InternalSettings {
    /// This id is used for internal handling of the inbox and is not persistent
    /// or unique across sessions.
    next_msg_id: u64,
    minimum_tier: Tier,
    /// Used for signing modifications to the state that are to be persisted.
    /// The public key must be the same as the one used for the inbox contract.
    private_key: RsaPrivateKey,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredDecryptedSettings {}

impl InternalSettings {
    fn from_stored(
        stored_settings: StoredSettings,
        next_id: u64,
        private_key: RsaPrivateKey,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            next_msg_id: next_id,
            private_key,
            minimum_tier: stored_settings.minimum_tier,
        })
    }

    fn to_stored(&self) -> Result<StoredSettings, Box<dyn std::error::Error>> {
        Ok(StoredSettings {
            minimum_tier: self.minimum_tier,
            private: vec![],
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct MessageModel {
    pub id: u64,
    pub content: DecryptedMessage,
    pub token_assignment: TokenAssignment,
}

impl MessageModel {
    fn to_stored(&self, key: &RsaPrivateKey) -> Result<StoredMessage, Box<dyn std::error::Error>> {
        // FIXME: use a real source of entropy
        let mut rng = rand_chacha::ChaChaRng::seed_from_u64(1);
        let decrypted_content = serde_json::to_vec(&self.content)?;
        let content = key
            .to_public_key()
            .encrypt(&mut rng, Pkcs1v15Encrypt, decrypted_content.as_ref())
            .map_err(|e| format!("{e}"))?;
        Ok::<_, Box<dyn std::error::Error>>(StoredMessage {
            content,
            token_assignment: self.token_assignment.clone(),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub(crate) struct DecryptedMessage {
    pub title: String,
    pub content: String,
    pub from: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub time: DateTime<Utc>,
}

/// Inbox state
#[derive(Debug, Clone)]
pub(crate) struct InboxModel {
    pub messages: Vec<MessageModel>,
    settings: InternalSettings,
    key: ContractKey,
}

impl InboxModel {
    fn new(private_key: RsaPrivateKey) -> Result<Self, Box<dyn std::error::Error>> {
        let params = InboxParams {
            pub_key: private_key.to_public_key(),
        };
        Ok(Self {
            messages: vec![],
            settings: InternalSettings {
                next_msg_id: 0,
                minimum_tier: Tier::Hour1,
                private_key,
            },
            key: ContractKey::from((&params.try_into()?, ContractCode::from([].as_slice()))),
        })
    }

    fn to_state(&self) -> Result<State<'static>, Box<dyn std::error::Error>> {
        let settings = self.settings.to_stored()?;
        let messages = self
            .messages
            .iter()
            .map(|m| m.to_stored(&self.settings.private_key))
            .collect::<Result<Vec<_>, _>>()?;
        let inbox = StoredInbox::new(&self.settings.private_key, settings, messages);
        let serialized = serde_json::to_vec(&inbox)?;
        Ok(serialized.into())
    }

    fn from_state(
        private_key: rsa::RsaPrivateKey,
        state: StoredInbox,
        key: ContractKey,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let messages = state
            .messages
            .iter()
            .enumerate()
            .map(|(id, msg)| {
                let decrypted_content = private_key
                    .decrypt(Pkcs1v15Encrypt, msg.content.as_ref())
                    .map_err(|e| format!("{e}"))?;
                let content: DecryptedMessage = serde_json::from_slice(&decrypted_content)?;
                Ok(MessageModel {
                    id: id as u64,
                    content,
                    token_assignment: msg.token_assignment.clone(),
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        Ok(Self {
            settings: InternalSettings::from_stored(
                state.settings,
                messages.len() as u64,
                private_key,
            )?,
            key,
            messages,
        })
    }

    /// This only affects in-memory messages, changes are not persisted.
    pub fn add_received_message(
        &mut self,
        content: DecryptedMessage,
        token_assignment: TokenAssignment,
    ) {
        self.messages.push(MessageModel {
            id: self.settings.next_msg_id,
            content,
            token_assignment,
        });
        self.settings.next_msg_id += 1;
    }

    /// This only affects in-memory messages, changes are not persisted.
    fn remove_received_message(&mut self, id: u64) {
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
        let signing_key = SigningKey::<Sha256>::new_with_prefix(self.settings.private_key.clone());
        let signature = signing_key.sign(&serialized).into();
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
        let signing_key = SigningKey::<Sha256>::new_with_prefix(self.settings.private_key.clone());
        let signature = signing_key.sign(&signed).into();
        let delta = UpdateInbox::RemoveMessages { signature, ids };
        let request = ContractRequest::Update {
            key: self.key.clone(),
            data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
        };
        client.send(request.into()).await?;
        Ok(())
    }

    #[cfg(feature = "node")]
    pub async fn add_messages_to_store(
        &mut self,
        client: &mut WebApi,
        ids: &[usize],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut signed = Vec::with_capacity(ids.len() * 32);
        let mut messages = Vec::with_capacity(ids.len() * 32);
        for m in &self.messages {
            let h = &m.token_assignment.assignment_hash;
            signed.extend(h);
            messages.push(m.to_stored(&self.settings.private_key)?);
        }
        let signing_key = SigningKey::<Sha256>::new_with_prefix(self.settings.private_key.clone());
        let signature = signing_key.sign(&signed).into();
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
        private_key: &RsaPrivateKey,
        key: ContractKey,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use locutus_stdlib::client_api::{ContractResponse, HostResponse};
        let request = ContractRequest::Get {
            key,
            fetch_contract: false,
        };
        client.send(request.into()).await?;
        match client.recv().await? {
            HostResponse::ContractResponse(ContractResponse::GetResponse { contract, state }) => {
                let Some(c) = contract else { panic!() };
                let state: StoredInbox = serde_json::from_slice(state.as_ref())?;
                Self::from_state(private_key.clone(), state, c.key())
            }
            _ => panic!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locutus_stdlib::prelude::ContractInstanceId;
    use rsa::pkcs1::DecodeRsaPrivateKey;

    use super::*;

    fn test_assignment() -> TokenAssignment {
        const RSA_PRIV_PEM: &str = include_str!("../examples/rsa4096-user-priv.pem");
        let key = RsaPrivateKey::from_pkcs1_pem(RSA_PRIV_PEM).unwrap();
        TokenAssignment {
            tier: Tier::Day1,
            time_slot: Default::default(),
            assignee: key.to_public_key(),
            signature: rsa::pkcs1v15::Signature::from(vec![1; 64].into_boxed_slice()),
            assignment_hash: [0; 32],
            token_record: ContractInstanceId::from_str(
                "7MxRGrYiBBK2rHCVpP25SxqBLco2h4zpb2szsTS7XXgg",
            )
            .unwrap(),
        }
    }

    #[test]
    fn remove_msg() {
        const RSA_PRIV_PEM: &str = include_str!("../examples/rsa4096-user-priv.pem");
        let key = RsaPrivateKey::from_pkcs1_pem(RSA_PRIV_PEM).unwrap();
        let mut inbox = InboxModel::new(key).unwrap();
        for id in 0..10000 {
            inbox.messages.push(MessageModel {
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
