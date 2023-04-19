use chacha20poly1305::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    XChaCha20Poly1305,
};
use chrono::{DateTime, NaiveDate, Utc};
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use locutus_aft_interface::{Tier, TokenAssignment};
use locutus_stdlib::client_api::{ClientRequest, DelegateRequest};
use locutus_stdlib::prelude::{
    ApplicationMessage, ContractInstanceId, DelegateKey, InboundDelegateMsg,
};
use locutus_stdlib::{
    client_api::ContractRequest,
    prelude::{ContractKey, State, UpdateData},
};
use rand_chacha::rand_core::SeedableRng;
use rsa::{
    pkcs1v15::{Signature, SigningKey},
    sha2::Sha256,
    signature::Signer,
    Pkcs1v15Encrypt, PublicKey, RsaPrivateKey, RsaPublicKey,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::app::{error_handling, TryAsyncAction};
use crate::{api::WebApiRequestClient, app::Identity, DynError};
use freenet_email_inbox::{
    Inbox as StoredInbox, InboxParams, InboxSettings as StoredSettings, Message as StoredMessage,
    UpdateInbox,
};

static INBOX_CODE_HASH: &str = include_str!("../examples/inbox_code_hash");

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
    fn to_stored(&self, token_assignment: TokenAssignment) -> Result<StoredMessage, DynError> {
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
        let encrypted_key = token_assignment
            .assignee
            .encrypt(&mut rng, Pkcs1v15Encrypt, &chacha_key)
            .map_err(|e| format!("{e}"))?;

        // Concatenate the nonce, encrypted XChaCha20Poly1305 key and encrypted data
        let mut content =
            Vec::with_capacity(chacha_nonce.len() + encrypted_key.len() + encrypted_data.len());
        content.extend(&chacha_nonce);
        content.extend(encrypted_key);
        content.extend(encrypted_data);

        Ok::<_, DynError>(StoredMessage {
            content,
            token_assignment,
        })
    }
}

/// Inbox state
#[derive(Debug, Clone)]
pub(crate) struct InboxModel {
    pub messages: Vec<MessageModel>,
    settings: InternalSettings,
    pub key: ContractKey,
}

impl InboxModel {
    pub(crate) async fn load(
        client: &mut WebApiRequestClient,
        contract: &Identity,
    ) -> Result<ContractKey, DynError> {
        let params = InboxParams {
            pub_key: contract.key.to_public_key(),
        }
        .try_into()
        .map_err(|e| format!("{e}"))?;
        let contract_key =
            ContractKey::from_params(INBOX_CODE_HASH, params).map_err(|e| format!("{e}"))?;
        InboxModel::get_state(client, contract_key.clone()).await?;
        Ok(contract_key)
    }

    pub(crate) async fn send_message(
        client: &mut WebApiRequestClient,
        content: DecryptedMessage,
        pub_key: RsaPublicKey,
    ) -> Result<(), DynError> {
        let token = {
            let key = pub_key.clone();
            //TODO: Use the component instead of hardcoding the TokenAssignment.
            //InboxModel::assign_token(client, key).await?
            const TEST_TIER: Tier = Tier::Day1;
            const MAX_DURATION_1Y: std::time::Duration =
                std::time::Duration::from_secs(365 * 24 * 3600);
            let naive = NaiveDate::from_ymd_opt(2023, 1, 25)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            let slot = DateTime::<Utc>::from_utc(naive, Utc);

            TokenAssignment {
                tier: TEST_TIER,
                time_slot: slot,
                assignee: key,
                signature: Signature::from(vec![1u8; 64].into_boxed_slice()),
                assignment_hash: [0; 32],
                token_record: ContractInstanceId::try_from(INBOX_CODE_HASH.to_string()).unwrap(),
            }
        };
        let params = InboxParams { pub_key }
            .try_into()
            .map_err(|e| format!("{e}"))?;
        let key = ContractKey::from_params(INBOX_CODE_HASH, params).map_err(|e| format!("{e}"))?;

        let delta = UpdateInbox::AddMessages {
            messages: vec![content.to_stored(token)?],
        };
        let request = ContractRequest::Update {
            key,
            data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
        };
        client.send(request.into()).await?;
        Ok(())
    }

    pub(crate) fn remove_messages(
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
                    TryAsyncAction::RemoveMessages,
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

    pub(crate) fn from_state(
        private_key: rsa::RsaPrivateKey,
        state: StoredInbox,
        key: ContractKey,
    ) -> Result<Self, DynError> {
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
    // TODO: call when new message updates come from the node
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
    ) -> Result<TokenAssignment, DynError> {
        let key = DelegateKey::new(&[]); // TODO: this should be the AFT component key
        let params = InboxParams {
            pub_key: recipient_key,
        }
        .try_into()?;
        let inbox_key = ContractKey::from_params(INBOX_CODE_HASH, params)?;
        let request = ClientRequest::DelegateOp(DelegateRequest::ApplicationMessages {
            key,
            inbound: vec![InboundDelegateMsg::ApplicationMessage(
                ApplicationMessage::new(inbox_key.into(), vec![]),
            )],
        });
        client.send(request).await?;
        todo!()
    }

    // async fn add_messages_to_store(
    //     &mut self,
    //     client: &mut WebApi,
    //     ids: &[usize],
    // ) -> Result<(), DynError> {
    //     let mut messages = Vec::with_capacity(ids.len() * 32);
    //     for message in &self.messages {
    //         // messages.push(message.to_stored(&self.settings.private_key)?);
    //     }
    //     let delta = UpdateInbox::AddMessages { messages };
    //     let request = ContractRequest::Update {
    //         key: self.key.clone(),
    //         data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
    //     };
    //     client.send(request.into()).await?;
    //     Ok(())
    // }

    async fn get_state(client: &mut WebApiRequestClient, key: ContractKey) -> Result<(), DynError> {
        let request = ContractRequest::Get {
            key,
            fetch_contract: false,
        };
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
        eprintln!("started {}", chrono::Utc::now());
        let t0 = std::time::Instant::now();
        for id in 2500..7500 {
            inbox.remove_received_message(&[id]);
        }
        eprintln!("{}ms", t0.elapsed().as_millis());
    }
}
