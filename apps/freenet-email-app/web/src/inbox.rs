use std::collections::HashSet;
use std::{cell::RefCell, rc::Rc};

use chrono::{DateTime, Utc};
use dioxus::prelude::*;
use freenet_email_inbox::{
    Inbox as StoredInbox, InboxParams, InboxSettings as StoredSettings, Message as StoredMessage,
    UpdateInbox,
};
use locutus_aft_interface::{Tier, TokenAssignment};
use locutus_stdlib::client_api::{ClientRequest, ComponentRequest};
use locutus_stdlib::prelude::{ApplicationMessage, ComponentKey, InboundComponentMsg};
use locutus_stdlib::{
    client_api::ContractRequest,
    prelude::{ContractKey, State, UpdateData},
};
use once_cell::unsync::Lazy;
use rand_chacha::rand_core::SeedableRng;
use rsa::RsaPublicKey;
use rsa::{
    pkcs1v15::SigningKey, sha2::Sha256, signature::Signer, Pkcs1v15Encrypt, PublicKey,
    RsaPrivateKey,
};
use serde::{Deserialize, Serialize};

use crate::app::Identity;
use crate::WebApi;

static INBOX_CODE_HASH: &str = include_str!("../examples/inbox_code_hash");

#[cfg(all(feature = "use-node", target_arch = "wasm32"))]
thread_local! {
    static CONNECTION: Lazy<Rc<RefCell<crate::WebApi>>> = Lazy::new(|| {
            let api = crate::WebApi::new()
                .map_err(|err| {
                    web_sys::console::error_1(&serde_wasm_bindgen::to_value(&err).unwrap());
                    err
                })
                .expect("open connection");
            Rc::new(RefCell::new(api))
    })
}

#[cfg(all(feature = "use-node", not(target_arch = "wasm32")))]
thread_local! {
    static CONNECTION: Lazy<Rc<RefCell<crate::WebApi>>> = Lazy::new(|| {
        unimplemented!()
    })
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

impl DecryptedMessage {
    fn to_stored(
        &self,
        token_assignment: TokenAssignment,
    ) -> Result<StoredMessage, Box<dyn std::error::Error>> {
        // FIXME: use a real source of entropy
        let mut rng = rand_chacha::ChaChaRng::seed_from_u64(1);
        let decrypted_content = serde_json::to_vec(self)?;
        let content = token_assignment
            .assignee
            .encrypt(&mut rng, Pkcs1v15Encrypt, decrypted_content.as_ref())
            .map_err(|e| format!("{e}"))?;
        Ok::<_, Box<dyn std::error::Error>>(StoredMessage {
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
    key: ContractKey,
}

impl InboxModel {
    pub(crate) fn load(cx: Scope, contract: &Identity) -> Result<Self, String> {
        let private_key = contract.key.clone();
        let params = InboxParams {
            pub_key: contract.key.to_public_key(),
        }
        .try_into()
        .map_err(|e| format!("{e}"))?;
        let contract_key =
            ContractKey::from_params(INBOX_CODE_HASH, params).map_err(|e| format!("{e}"))?;
        CONNECTION.with(|conn| {
            let client = (**conn).clone();
            // FIXME: properly set this future to re-evaluate on change, now it would only send once
            let f = use_future(cx, (), |_| async move {
                let client = &mut *client.borrow_mut();
                let state = InboxModel::get_state(client, contract_key.clone()).await?;
                Self::from_state(private_key, state, contract_key)
            });
            let inbox = loop {
                match f.value() {
                    Some(v) => break v.as_ref().unwrap(),
                    None => std::thread::sleep(std::time::Duration::from_micros(100)),
                }
            };
            Ok::<_, String>(inbox.clone())
        })
    }
    pub(crate) fn send_message(
        cx: Scope,
        content: DecryptedMessage,
        pub_key: RsaPublicKey,
    ) -> Result<(), Box<dyn std::error::Error>> {
        CONNECTION.with(|conn| {
            let client = (**conn).clone();
            let token = {
                // FIXME: properly set this future to re-evaluate on change, now it would only send once
                let key = pub_key.clone();
                let f = use_future(cx, (), |_| async move {
                    let client = &mut *client.borrow_mut();
                    InboxModel::assign_token(client, key).await
                });
                loop {
                    match f.value() {
                        Some(Ok(r)) => break r.clone(),
                        Some(Err(e)) => return Err(format!("{e}").into()),
                        None => std::thread::sleep(std::time::Duration::from_micros(100)),
                    }
                }
            };
            // FIXME: properly set this future to re-evaluate on change, now it would only send once
            let client = (**conn).clone();
            let params = InboxParams { pub_key }
                .try_into()
                .map_err(|e| format!("{e}"))?;
            let key =
                ContractKey::from_params(INBOX_CODE_HASH, params).map_err(|e| format!("{e}"))?;
            let f = use_future(cx, (), |_| async move {
                let client = &mut *client.borrow_mut();
                let delta = UpdateInbox::AddMessages {
                    messages: vec![content.to_stored(token)?],
                };
                let request = ContractRequest::Update {
                    key,
                    data: UpdateData::Delta(serde_json::to_vec(&delta)?.into()),
                };
                client.send(request.into()).await?;
                Ok::<_, Box<dyn std::error::Error>>(())
            });
            loop {
                match f.value() {
                    Some(Err(e)) => {
                        break Err::<(), Box<dyn std::error::Error>>(format!("{e}").into())
                    }
                    Some(_) => {}
                    None => std::thread::sleep(std::time::Duration::from_micros(100)),
                }
            }
        })
    }

    pub(crate) fn remove_messages<T>(
        this: Rc<RefCell<Self>>,
        cx: Scope<T>,
        ids: &[u64],
    ) -> Result<(), Box<dyn std::error::Error>> {
        this.borrow_mut().remove_received_message(ids);
        let ids = ids.to_vec();
        CONNECTION.with(|conn| {
            let client = (**conn).clone();
            // FIXME: properly set this future to re-evaluate on change, now it would only send once
            let f = use_future(cx, (), |_| async move {
                let client = &mut *client.borrow_mut();
                this.borrow_mut()
                    .remove_messages_from_store(client, &ids)
                    .await?;
                Ok::<_, Box<dyn std::error::Error>>(())
            });
            loop {
                match f.value() {
                    Some(Err(e)) => {
                        break Err::<(), Box<dyn std::error::Error>>(format!("{e}").into())
                    }
                    Some(_) => {}
                    None => std::thread::sleep(std::time::Duration::from_micros(100)),
                }
            }
        })
    }

    // TODO: only used when an inbox is created first time
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
        ids: &[u64],
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

    async fn assign_token(
        client: &mut WebApi,
        recipient_key: RsaPublicKey,
    ) -> Result<TokenAssignment, Box<dyn std::error::Error>> {
        let key = ComponentKey::new(&[]); // TODO: this should be the AFT component key
        let params = InboxParams {
            pub_key: recipient_key,
        }
        .try_into()?;
        let inbox_key = ContractKey::from_params(INBOX_CODE_HASH, params)?;
        let request = ClientRequest::ComponentOp(ComponentRequest::ApplicationMessages {
            key,
            inbound: vec![InboundComponentMsg::ApplicationMessage(
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
    // ) -> Result<(), Box<dyn std::error::Error>> {
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

    async fn get_state(
        client: &mut WebApi,
        key: ContractKey,
    ) -> Result<StoredInbox, Box<dyn std::error::Error>> {
        use locutus_stdlib::client_api::{ContractResponse, HostResponse};
        let request = ContractRequest::Get {
            key,
            fetch_contract: false,
        };
        client.send(request.into()).await?;
        match client.recv().await? {
            HostResponse::ContractResponse(ContractResponse::GetResponse { state, .. }) => {
                let state: StoredInbox = serde_json::from_slice(state.as_ref())?;
                Ok(state)
            }
            _ => panic!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use locutus_stdlib::prelude::ContractCode;
    use rsa::pkcs1::DecodeRsaPrivateKey;

    use super::*;

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
