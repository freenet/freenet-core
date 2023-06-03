use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    io::{Cursor, Read},
    sync::atomic::AtomicU32,
};

use chacha20poly1305::aead::generic_array::GenericArray;
use chacha20poly1305::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    XChaCha20Poly1305,
};
use chrono::{DateTime, Utc};
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use locutus_aft_interface::{
    AllocationCriteria, RequestNewToken, Tier, TokenAllocationRecord, TokenAssignment,
    TokenDelegateMessage, TokenParameters,
};
use locutus_stdlib::{
    client_api::{ClientRequest, ContractRequest, DelegateRequest},
    prelude::{
        blake2::{self, Digest},
        ApplicationMessage, ContractInstanceId, ContractKey, DelegateKey, InboundDelegateMsg,
        Parameters, State, UpdateData,
    },
};
use rand_chacha::rand_core::SeedableRng;
use rsa::{
    pkcs1::EncodeRsaPublicKey, pkcs1v15::SigningKey, sha2::Sha256, signature::Signer,
    Pkcs1v15Encrypt, PublicKey, RsaPrivateKey, RsaPublicKey,
};
use serde::{Deserialize, Serialize};

use crate::{
    aft::AftRecords,
    api::WebApiRequestClient,
    app::{error_handling, Identity, TryNodeAction},
    DynError,
};
use freenet_email_inbox::{
    Inbox as StoredInbox, InboxParams, InboxSettings as StoredSettings, Message as StoredMessage,
    UpdateInbox,
};

static INBOX_CODE_HASH: &str = include_str!("../build/inbox_code_hash");

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
        // FIXME: use a real source of entropy
        let mut rng = rand_chacha::ChaChaRng::seed_from_u64(1);
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

impl DecryptedMessage {
    fn to_stored(&self, mut token_assignment: TokenAssignment) -> Result<StoredMessage, DynError> {
        let (hash, content) =
            self.assignment_hash_and_signed_content(&token_assignment.assignee)?;
        token_assignment.assignment_hash = hash;
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

        let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(&chacha_key));
        let decrypted_content = cipher
            .decrypt(GenericArray::from_slice(nonce.as_ref()), content.as_ref())
            .map_err(|e| format!("{e}"))
            .unwrap();
        let content: DecryptedMessage = serde_json::from_slice(&decrypted_content).unwrap();
        content
    }

    fn assignment_hash_and_signed_content(
        &self,
        assignee: &RsaPublicKey,
    ) -> Result<([u8; 32], Vec<u8>), DynError> {
        // FIXME: use a real source of entropy
        let mut rng = rand_chacha::ChaChaRng::seed_from_u64(1);
        let decrypted_content: Vec<u8> = serde_json::to_vec(self)?;

        // Generate a random 256-bit XChaCha20Poly1305 key
        let chacha_key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let chacha_nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);

        // Encrypt the data using XChaCha20Poly1305
        let cipher = XChaCha20Poly1305::new(&chacha_key);
        let encrypted_data = cipher
            .encrypt(&chacha_nonce, decrypted_content.as_slice())
            .unwrap();

        // Encrypt the XChaCha20Poly1305 key using RSA
        let encrypted_key = assignee
            .encrypt(&mut rng, Pkcs1v15Encrypt, &chacha_key)
            .map_err(|e| format!("{e}"))?;

        // Concatenate the nonce, encrypted XChaCha20Poly1305 key and encrypted data
        let mut content =
            Vec::with_capacity(chacha_nonce.len() + encrypted_key.len() + encrypted_data.len());
        content.extend(&chacha_nonce);
        content.extend(encrypted_key);
        content.extend(encrypted_data);

        let mut hasher = blake2::Blake2s256::new();
        hasher.update(&content);
        let assignment_hash: [u8; 32] = hasher.finalize().as_slice().try_into().unwrap();
        Ok((assignment_hash, content))
    }
}

thread_local! {
    static INBOX_TO_ID: RefCell<HashMap<ContractKey, Identity>> =
        RefCell::new(HashMap::new());
}

/// Inbox state
#[derive(Debug, Clone)]
pub(crate) struct InboxModel {
    pub messages: Vec<MessageModel>,
    settings: InternalSettings,
    pub key: ContractKey,
}

impl InboxModel {
    pub async fn load_all(
        client: &mut WebApiRequestClient,
        contracts: &[Identity],
        contract_to_id: &mut HashMap<ContractKey, Identity>,
    ) {
        async fn subscribe(
            client: &mut WebApiRequestClient,
            identity: &Identity,
        ) -> Result<(), DynError> {
            let pub_key = identity.key.to_public_key();
            let alias = crate::app::ALIAS_MAP2
                .get(&pub_key.to_pkcs1_pem(rsa::pkcs1::LineEnding::LF).unwrap())
                .unwrap();
            let params = freenet_email_inbox::InboxParams { pub_key }
                .try_into()
                .map_err(|e| format!("{e}"))?;
            let contract_key = ContractKey::from_params(crate::inbox::INBOX_CODE_HASH, params)
                .map_err(|e| format!("{e}"))?;
            {
                INBOX_TO_ID.with(|map| {
                    map.borrow_mut()
                        .insert(contract_key.clone(), identity.clone());
                });
            }
            crate::log::log(format!(
                "subscribed to inbox updates for `{contract_key}`, belonging to alias `{alias}`"
            ));
            InboxModel::subscribe(client, contract_key.clone()).await?;
            Ok(())
        }

        for identity in contracts {
            let mut client = client.clone();
            let res = subscribe(&mut client, identity).await;
            error_handling(client.clone().into(), res, TryNodeAction::LoadInbox).await;
            let res = InboxModel::load(&mut client, identity).await.map(|key| {
                contract_to_id
                    .entry(key.clone())
                    .or_insert(identity.clone());
                key
            });
            error_handling(client.into(), res.map(|_| ()), TryNodeAction::LoadInbox).await;
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

    pub fn contract_identity(key: &ContractKey) -> Option<Identity> {
        INBOX_TO_ID.with(|map| map.borrow().get(key).cloned())
    }

    pub async fn send_message(
        client: &mut WebApiRequestClient,
        content: DecryptedMessage,
        recipient_key: RsaPublicKey,
        from: &Identity,
    ) -> Result<(), DynError> {
        let key = recipient_key.clone();
        let (hash, _) = content.assignment_hash_and_signed_content(&recipient_key)?;
        let delegate_key = InboxModel::assign_token(client, key, from, hash).await?;
        let params = InboxParams {
            pub_key: recipient_key,
        }
        .try_into()
        .map_err(|e| format!("{e}"))?;
        let inbox_key =
            ContractKey::from_params(INBOX_CODE_HASH, params).map_err(|e| format!("{e}"))?;
        AftRecords::pending_assignment(delegate_key, inbox_key);
        // let delta = UpdateInbox::AddMessages {
        //     messages: vec![content.to_stored(token)?],
        // };
        // let request = ContractRequest::Update {
        //     key,
        //     data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
        // };
        // client.send(request.into()).await?;
        Ok(())
    }

    pub fn remove_messages(
        &mut self,
        mut client: WebApiRequestClient,
        ids: &[u64],
    ) -> Result<LocalBoxFuture<'static, ()>, DynError> {
        self.remove_received_message(ids);
        let ids = ids.to_vec();
        let mut signed: Vec<u8> = Vec::with_capacity(ids.len() * 32);
        let mut ids = Vec::with_capacity(ids.len() * 32);
        for m in &self.messages {
            let h = &m.token_assignment.assignment_hash;
            signed.extend(h);
            ids.push(*h);
        }
        #[cfg(feature = "use-node")]
        {
            let signing_key =
                SigningKey::<Sha256>::new_with_prefix(self.settings.private_key.clone());
            let signature = signing_key.sign(&signed).into();
            let delta = UpdateInbox::RemoveMessages { signature, ids };
            let request = ContractRequest::Update {
                key: self.key.clone(),
                data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
            };
            let f = async move {
                let r = client.send(request.into()).await;
                error_handling(
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

    async fn assign_token(
        client: &mut WebApiRequestClient,
        recipient_key: RsaPublicKey,
        generator_id: &Identity,
        assignment_hash: [u8; 32],
    ) -> Result<DelegateKey, DynError> {
        static REQUEST_ID: AtomicU32 = AtomicU32::new(0);
        let inbox_params: Parameters = InboxParams {
            pub_key: recipient_key.clone(),
        }
        .try_into()?;
        let delegate_key = DelegateKey::from_params(
            crate::aft::TOKEN_GENERATOR_DELEGATE_CODE_HASH,
            inbox_params.clone(),
        )?;
        let inbox_key = ContractKey::from_params(INBOX_CODE_HASH, inbox_params.clone())?;
        let delegate_params =
            locutus_aft_interface::DelegateParameters::new(generator_id.key.clone());

        let record_params = TokenParameters::new(generator_id.key.to_public_key());
        let token_record: ContractInstanceId = ContractKey::from_params(
            crate::aft::TOKEN_RECORD_CODE_HASH,
            record_params.try_into()?,
        )
        .unwrap()
        .into();
        // todo: the criteria should come from the recipient inbox really
        let criteria = AllocationCriteria::new(
            Tier::Day1,
            std::time::Duration::from_secs(365 * 24 * 3600),
            token_record,
        )?;
        // fixme: should be using the state of the aft record contract here instead:
        let records = TokenAllocationRecord::new(HashMap::default());
        let token_request = TokenDelegateMessage::RequestNewToken(RequestNewToken {
            request_id: REQUEST_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            delegate_id: delegate_key.clone().into(),
            criteria,
            records,
            assignee: recipient_key.clone(),
            assignment_hash,
        });
        let request = ClientRequest::DelegateOp(DelegateRequest::ApplicationMessages {
            key: delegate_key.clone(),
            params: delegate_params.try_into()?,
            inbound: vec![InboundDelegateMsg::ApplicationMessage(
                ApplicationMessage::new(inbox_key.into(), token_request.serialize()?),
            )],
        });
        client.send(request).await?;

        // let start = Utc::now();
        // let mut token = None;
        // let timeout = chrono::Duration::milliseconds(500);
        // while (Utc::now() - start) > timeout {
        //     crate::log::log(format!("waiting... {}", Utc::now()));
        //     token = AftRecords::recv_token(&delegate_key).await;
        // }
        // token
        //     .ok_or_else(|| {
        //         let err = format!(
        //             "failed trying to get a token for `{}`",
        //             generator_id.alias()
        //         );
        //         crate::log::error(&err, Some(TryNodeAction::SendMessage));
        //         err.into()
        //     })
        //     .map(|t| (delegate_key, t))

        // const TEST_TIER: Tier = Tier::Day1;
        // let naive = chrono::NaiveDate::from_ymd_opt(2023, 1, 25)
        //     .unwrap()
        //     .and_hms_opt(0, 0, 0)
        //     .unwrap();
        // let slot = DateTime::<Utc>::from_utc(naive, Utc);
        // crate::log::log(
        //     format!(
        //         "Sending update request message with token record key: {}",
        //         token_record.clone()
        //     )
        //     .as_str(),
        // );
        // Ok((
        //     delegate_key,
        //     TokenAssignment {
        //         tier: TEST_TIER,
        //         time_slot: slot,
        //         assignee: recipient_key,
        //         signature: rsa::pkcs1v15::Signature::from(vec![1u8; 64].into_boxed_slice()),
        //         assignment_hash: [0; 32],
        //         token_record,
        //     },
        // ))
        Ok(delegate_key)
    }

    async fn get_state(client: &mut WebApiRequestClient, key: ContractKey) -> Result<(), DynError> {
        let request = ContractRequest::Get {
            key,
            fetch_contract: false,
        };
        client.send(request.into()).await?;
        Ok(())
    }

    async fn subscribe(client: &mut WebApiRequestClient, key: ContractKey) -> Result<(), DynError> {
        // todo: send the proper summary from the current state
        let request = ContractRequest::Subscribe { key, summary: None };
        client.send(request.into()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use locutus_stdlib::prelude::ContractCode;
    use rsa::pkcs1::DecodeRsaPrivateKey;

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
                key: ContractKey::from((&params.try_into()?, ContractCode::from([].as_slice()))),
            })
        }
    }

    #[test]
    fn remove_msg() {
        const RSA_PRIV_PEM: &str = include_str!("../examples/rsa4096-id-1-priv.pem");
        let key = RsaPrivateKey::from_pkcs1_pem(RSA_PRIV_PEM).unwrap();
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
