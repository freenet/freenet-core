use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    io::{Cursor, Read},
};

use chacha20poly1305::aead::generic_array::GenericArray;
use chacha20poly1305::{
    aead::{Aead, AeadCore, OsRng},
    XChaCha20Poly1305,
};
use chrono::{DateTime, Utc};
use freenet_aft_interface::{Tier, TokenAssignment, TokenAssignmentHash};
use freenet_stdlib::prelude::StateSummary;
use freenet_stdlib::{
    client_api::ContractRequest,
    prelude::{
        blake3::{self, traits::digest::Digest},
        ContractKey, State, UpdateData,
    },
};
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use rsa::{
    pkcs1v15::SigningKey, sha2::Sha256, signature::Signer, Pkcs1v15Encrypt, RsaPrivateKey,
    RsaPublicKey,
};
use serde::{Deserialize, Serialize};

use freenet_email_inbox::{
    Inbox as StoredInbox, InboxParams, InboxSettings as StoredSettings, Message as StoredMessage,
    UpdateInbox,
};

use crate::{
    aft::AftRecords,
    api::{node_response_error_handling, TryNodeAction, WebApiRequestClient},
    app::Identity,
    DynError,
};

type InboxContract = ContractKey;

pub(crate) const INBOX_CODE_HASH: &str = include_str!("../build/inbox_code_hash");

thread_local! {
    static PENDING_INBOXES_UPDATE: RefCell<HashMap<InboxContract, Vec<DecryptedMessage>>> = RefCell::new(HashMap::new());
    static INBOX_TO_ID: RefCell<HashMap<InboxContract, Identity>> =
        RefCell::new(HashMap::new());
}

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

#[derive(Serialize, Deserialize)]
struct InboxSummary(HashSet<TokenAssignmentHash>);

impl InboxSummary {
    pub fn new(messages: HashSet<TokenAssignmentHash>) -> Self {
        Self(messages)
    }
}

impl InternalSettings {
    fn from_stored(
        stored_settings: StoredSettings,
        next_id: u64,
        private_key: RsaPrivateKey,
    ) -> Result<Self, DynError> {
        Ok(Self {
            next_msg_id: next_id,
            private_key,
            minimum_tier: stored_settings.minimum_tier,
        })
    }

    fn to_stored(&self) -> Result<StoredSettings, DynError> {
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
    fn to_stored(&self, key: &RsaPrivateKey) -> Result<StoredMessage, DynError> {
        let mut rng = OsRng;
        let decrypted_content = serde_json::to_vec(&self.content)?;
        let content = key
            .to_public_key()
            .encrypt(&mut rng, Pkcs1v15Encrypt, decrypted_content.as_ref())
            .map_err(|e| format!("{e}"))?;
        Ok::<_, DynError>(StoredMessage {
            content,
            token_assignment: self.token_assignment.clone(),
        })
    }

    pub async fn finish_sending(
        client: &mut WebApiRequestClient,
        assignment: TokenAssignment,
        inbox_contract: InboxContract,
    ) -> Result<(), DynError> {
        let pending_update = PENDING_INBOXES_UPDATE.with(|map| {
            let map = &mut *map.borrow_mut();
            let update = map.get_mut(&inbox_contract).and_then(|messages| {
                if !messages.is_empty() {
                    Some(messages.remove(0))
                } else {
                    None
                }
            });
            if let Some(messages) = map.get(&inbox_contract) {
                if messages.is_empty() {
                    map.remove(&inbox_contract);
                }
            }
            update
        });

        if let Some(update) = pending_update {
            let delta = UpdateInbox::AddMessages {
                messages: vec![update.to_stored(assignment)?],
            };
            let request = ContractRequest::Update {
                key: inbox_contract,
                data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
            };
            client.send(request.into()).await?;
            // todo: event after sending, we may fail to update, must keep this in mind in case we receive no confirmation
            // meaning that we will need to retry, and likely need an other token for now at least, so restart from 0
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub(crate) struct DecryptedMessage {
    pub title: String,
    pub content: String,
    pub from: String,
    pub to: Vec<RsaPublicKey>,
    pub cc: Vec<String>,
    pub time: DateTime<Utc>,
}

impl DecryptedMessage {
    pub async fn start_sending(
        self,
        client: &mut WebApiRequestClient,
        recipient_key: RsaPublicKey,
        from: &Identity,
    ) -> Result<(), DynError> {
        let (hash, _) = self.assignment_hash_and_signed_content()?;
        crate::log::debug!(
            "requesting token for assignment hash: {}",
            bs58::encode(hash).into_string()
        );
        let delegate_key =
            AftRecords::assign_token(client, recipient_key.clone(), from, hash).await?;
        let params = InboxParams {
            pub_key: recipient_key,
        }
        .try_into()
        .map_err(|e| format!("{e}"))?;
        let inbox_key = ContractKey::from_params(INBOX_CODE_HASH, params).map_err(|e| format!("{e}"))?;
        AftRecords::pending_assignment(delegate_key, inbox_key.clone());

        PENDING_INBOXES_UPDATE.with(|map| {
            let map = &mut *map.borrow_mut();
            map.entry(inbox_key).or_insert_with(Vec::new).push(self);
        });
        Ok(())
    }

    fn to_stored(&self, token_assignment: TokenAssignment) -> Result<StoredMessage, DynError> {
        let (_, content) = self.assignment_hash_and_signed_content()?;
        Ok::<_, DynError>(StoredMessage {
            content,
            token_assignment,
        })
    }

    fn from_stored(private_key: &RsaPrivateKey, msg_content: Vec<u8>) -> DecryptedMessage {
        let mut msg_cursor = Cursor::new(msg_content);
        let mut nonce = vec![0; 24];
        msg_cursor.read_exact(&mut nonce).unwrap();
        let mut encrypted_chacha_key = vec![0; 512];
        msg_cursor.read_exact(&mut encrypted_chacha_key).unwrap();
        let mut content = vec![];
        msg_cursor.read_to_end(&mut content).unwrap();

        let chacha_key = private_key
            .decrypt(Pkcs1v15Encrypt, encrypted_chacha_key.as_ref())
            .map_err(|e| format!("{e}"))
            .unwrap();

        use chacha20poly1305::aead::KeyInit;
        let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(&chacha_key));
        let decrypted_content = cipher
            .decrypt(GenericArray::from_slice(nonce.as_ref()), content.as_ref())
            .map_err(|e| format!("{e}"))
            .unwrap();
        let content: DecryptedMessage = serde_json::from_slice(&decrypted_content).unwrap();
        content
    }

    fn assignment_hash_and_signed_content(&self) -> Result<([u8; 32], Vec<u8>), DynError> {
        let mut rng = OsRng;
        let decrypted_content: Vec<u8> = serde_json::to_vec(self)?;

        // Generate a random 256-bit XChaCha20Poly1305 key
        let chacha_key = {
            use chacha20poly1305::aead::KeyInit;
            XChaCha20Poly1305::generate_key(&mut OsRng)
        };
        let chacha_nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);

        // Encrypt the data using XChaCha20Poly1305
        let cipher = {
            use chacha20poly1305::aead::KeyInit;
            XChaCha20Poly1305::new(&chacha_key)
        };
        let encrypted_data = cipher
            .encrypt(&chacha_nonce, decrypted_content.as_slice())
            .unwrap();

        // Encrypt the XChaCha20Poly1305 key using RSA
        let receiver_pub_key = self.to.get(0).ok_or("receiver key not found")?;
        let encrypted_key = receiver_pub_key
            .encrypt(&mut rng, Pkcs1v15Encrypt, chacha_key.as_slice())
            .map_err(|e| format!("{e}"))?;

        // Concatenate the nonce, encrypted XChaCha20Poly1305 key and encrypted data
        let mut content =
            Vec::with_capacity(chacha_nonce.len() + encrypted_key.len() + encrypted_data.len());
        content.extend(&chacha_nonce);
        content.extend(encrypted_key);
        content.extend(encrypted_data);

        let mut hasher = blake3::Hasher::new();
        hasher.update(&content);
        let assignment_hash: [u8; 32] = hasher.finalize().as_slice().try_into().unwrap();
        Ok((assignment_hash, content))
    }
}

/// Inbox state
#[derive(Debug, Clone)]
pub(crate) struct InboxModel {
    pub messages: Vec<MessageModel>,
    settings: InternalSettings,
    pub key: InboxContract,
}

impl InboxModel {
    pub async fn load_all(
        client: &mut WebApiRequestClient,
        contracts: &[Identity],
        contract_to_id: &mut HashMap<InboxContract, Identity>,
    ) {
        async fn subscribe(
            client: &mut WebApiRequestClient,
            contract_key: &ContractKey,
            identity: &Identity,
        ) -> Result<(), DynError> {
            let alias = identity.alias();
            INBOX_TO_ID.with(|map| {
                map.borrow_mut()
                    .insert(contract_key.clone(), identity.clone());
            });
            crate::log::debug!(
                "subscribing to inbox updates for `{contract_key}`, belonging to alias `{alias}`"
            );
            InboxModel::subscribe(client, contract_key.clone()).await?;
            Ok(())
        }

        fn get_key(identity: &Identity) -> Result<ContractKey, DynError> {
            let pub_key = identity.key.to_public_key();
            let params = freenet_email_inbox::InboxParams { pub_key }
                .try_into()
                .map_err(|e| format!("{e}"))?;
            ContractKey::from_params(crate::inbox::INBOX_CODE_HASH, params)
                .map_err(|e| format!("{e}").into())
        }

        for identity in contracts {
            let mut client = client.clone();
            let contract_key = match get_key(identity) {
                Ok(v) => v,
                Err(e) => {
                    node_response_error_handling(
                        client.clone().into(),
                        Err(e),
                        TryNodeAction::LoadInbox,
                    )
                    .await;
                    return;
                }
            };
            if !contract_to_id.contains_key(&contract_key) {
                let res = subscribe(&mut client, &contract_key, identity).await;
                node_response_error_handling(client.clone().into(), res, TryNodeAction::LoadInbox)
                    .await;
            }
            let res = InboxModel::load(&mut client, identity).await.map(|key| {
                contract_to_id
                    .entry(key.clone())
                    .or_insert(identity.clone());
                key
            });
            node_response_error_handling(client.into(), res.map(|_| ()), TryNodeAction::LoadInbox)
                .await;
        }
    }

    pub async fn load(
        client: &mut WebApiRequestClient,
        id: &Identity,
    ) -> Result<ContractKey, DynError> {
        let params = InboxParams {
            pub_key: id.key.to_public_key(),
        }
        .try_into()
        .map_err(|e| format!("{e}"))?;
        let contract_key =
            ContractKey::from_params(INBOX_CODE_HASH, params).map_err(|e| format!("{e}"))?;
        InboxModel::get_state(client, contract_key.clone()).await?;
        Ok(contract_key)
    }

    pub fn id_for_alias(alias: &str) -> Option<Identity> {
        INBOX_TO_ID.with(|map| {
            map.borrow()
                .values()
                .find_map(|id| (id.alias() == alias).then(|| id.clone()))
        })
    }

    pub fn contract_identity(key: &InboxContract) -> Option<Identity> {
        INBOX_TO_ID.with(|map| map.borrow().get(key).cloned())
    }

    pub fn set_contract_identity(key: InboxContract, identity: Identity) {
        crate::log::debug!(
            "adding inbox contract for `{alias}` ({key})",
            alias = identity.alias
        );
        INBOX_TO_ID.with(|map| map.borrow_mut().insert(key, identity));
    }

    pub fn remove_messages(
        &mut self,
        mut client: WebApiRequestClient,
        ids: &[u64],
    ) -> Result<LocalBoxFuture<'static, ()>, DynError> {
        let mut signed: Vec<u8> = Vec::with_capacity(ids.len() * 32);
        let mut to_rm_message_id = Vec::with_capacity(ids.len() * 32);
        for m in &self.messages {
            if ids.contains(&m.id) {
                let h = &m.token_assignment.assignment_hash;
                signed.extend(h);
                to_rm_message_id.push(*h);
            }
        }
        self.remove_received_message(ids);
        #[cfg(feature = "use-node")]
        {
            let signing_key = SigningKey::<Sha256>::new(self.settings.private_key.clone());
            let signature: Box<[u8]> = signing_key.sign(&signed).into();
            let delta: UpdateInbox = UpdateInbox::RemoveMessages {
                signature,
                ids: to_rm_message_id,
            };
            let request = ContractRequest::Update {
                key: self.key.clone(),
                data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
            };
            let f = async move {
                let r = client.send(request.into()).await;
                node_response_error_handling(
                    client.into(),
                    r.map_err(Into::into),
                    TryNodeAction::RemoveMessages,
                )
                .await;
            };
            Ok(f.boxed_local())
        }
        #[cfg(not(feature = "use-node"))]
        {
            Ok(async {}.boxed_local())
        }
    }

    pub fn merge(&mut self, other: InboxModel) {
        for m in other.messages {
            if !self
                .messages
                .iter()
                .any(|c| c.content.time == m.content.time)
            {
                self.add_received_message(m.content, m.token_assignment);
            }
        }
    }

    // TODO: only used when an inbox is created first time when putting the contract
    fn to_state(&self) -> Result<State<'static>, DynError> {
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

    pub fn from_state(
        private_key: rsa::RsaPrivateKey,
        state: StoredInbox,
        key: ContractKey,
    ) -> Result<Self, DynError> {
        let messages = state
            .messages
            .iter()
            .enumerate()
            .map(|(id, msg)| {
                let content = DecryptedMessage::from_stored(&private_key, msg.content.clone());
                Ok(MessageModel {
                    id: id as u64,
                    content,
                    token_assignment: msg.token_assignment.clone(),
                })
            })
            .collect::<Result<Vec<_>, DynError>>()?;
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
    fn add_received_message(
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
    fn remove_received_message(&mut self, ids: &[u64]) {
        if ids.len() > 1 {
            let drop: HashSet<u64> = HashSet::from_iter(ids.iter().copied());
            self.messages.retain(|a| !drop.contains(&a.id));
        } else {
            for id in ids {
                if let Ok(p) = self.messages.binary_search_by_key(id, |a| a.id) {
                    self.messages.remove(p);
                }
            }
        }
    }

    async fn update_settings_at_store(
        &mut self,
        client: &mut WebApiRequestClient,
    ) -> Result<(), DynError> {
        let settings = self.settings.to_stored()?;
        let serialized = serde_json::to_vec(&settings)?;
        let signing_key = SigningKey::<Sha256>::new(self.settings.private_key.clone());
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

    async fn get_state(client: &mut WebApiRequestClient, key: ContractKey) -> Result<(), DynError> {
        let request = ContractRequest::Get {
            key,
            fetch_contract: false,
        };
        client.send(request.into()).await?;
        Ok(())
    }

    pub async fn subscribe(
        client: &mut WebApiRequestClient,
        key: ContractKey,
    ) -> Result<(), DynError> {
        // todo: send the proper summary from the current state
        let summary: StateSummary = serde_json::to_vec(&InboxSummary::new(HashSet::new()))?.into();
        let request = ContractRequest::Subscribe {
            key,
            summary: Some(summary),
        };
        client.send(request.into()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use freenet_stdlib::prelude::ContractCode;

    use super::*;

    impl InboxModel {
        fn new(private_key: RsaPrivateKey) -> Result<Self, DynError> {
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
                key: ContractKey::from_params_and_code(
                    &params.try_into()?,
                    ContractCode::from([].as_slice()),
                ),
            })
        }
    }

    #[test]
    fn remove_msg() {
        let key = RsaPrivateKey::new(&mut OsRng, 32).unwrap();
        let mut inbox = InboxModel::new(key).unwrap();
        for id in 0..10000 {
            inbox.messages.push(MessageModel {
                id,
                content: DecryptedMessage::default(),
                token_assignment: crate::test_util::test_assignment(),
            });
        }
        let t0 = std::time::Instant::now();
        for id in 2500..7500 {
            inbox.remove_received_message(&[id]);
        }
        eprintln!("{}ms", t0.elapsed().as_millis());
    }
}
