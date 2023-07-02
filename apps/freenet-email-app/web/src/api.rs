use std::{cell::RefCell, collections::HashMap, sync::OnceLock};

use dioxus::prelude::{UnboundedReceiver, UnboundedSender};
use futures::SinkExt;
use locutus_aft_interface::{TokenAllocationSummary, TokenDelegateMessage};
use locutus_stdlib::client_api::{ClientError, ClientRequest, HostResponse};

use crate::DynError;

type ClientRequester = UnboundedSender<ClientRequest<'static>>;
type HostResponses = UnboundedReceiver<Result<HostResponse, ClientError>>;

pub(crate) type NodeResponses = UnboundedSender<AsyncActionResult>;

pub(crate) static WEB_API_SENDER: OnceLock<WebApiRequestClient> = OnceLock::new();

#[cfg(feature = "use-node")]
struct WebApi {
    requests: UnboundedReceiver<ClientRequest<'static>>,
    host_responses: HostResponses,
    client_errors: UnboundedReceiver<AsyncActionResult>,
    send_half: ClientRequester,
    error_sender: NodeResponses,
    api: locutus_stdlib::client_api::WebApi,
    connecting: Option<futures::channel::oneshot::Receiver<()>>,
}

#[cfg(not(feature = "use-node"))]
struct WebApi {}

impl WebApi {
    #[cfg(not(feature = "use-node"))]
    fn new() -> Result<Self, String> {
        Ok(Self {})
    }

    #[cfg(all(not(target_family = "wasm"), feature = "use-node"))]
    fn new() -> Result<Self, String> {
        todo!()
    }

    #[cfg(all(target_family = "wasm", feature = "use-node"))]
    fn new() -> Result<Self, String> {
        use futures::{SinkExt, StreamExt};
        let conn = web_sys::WebSocket::new("ws://localhost:50509/contract/command/").unwrap();
        let (send_host_responses, host_responses) = futures::channel::mpsc::unbounded();
        let (send_half, requests) = futures::channel::mpsc::unbounded();
        let result_handler = move |result: Result<HostResponse, ClientError>| {
            let mut send_host_responses_clone = send_host_responses.clone();
            let _ = wasm_bindgen_futures::future_to_promise(async move {
                send_host_responses_clone
                    .send(result)
                    .await
                    .expect("channel open");
                Ok(wasm_bindgen::JsValue::NULL)
            });
        };
        let (tx, rx) = futures::channel::oneshot::channel();
        let onopen_handler = move || {
            let _ = tx.send(());
            crate::log::debug!("connected to websocket");
        };
        let mut api = locutus_stdlib::client_api::WebApi::start(
            conn,
            result_handler,
            |err| {
                crate::log::error(format!("host error: {err}"), None);
            },
            onopen_handler,
        );
        let (error_sender, client_errors) = futures::channel::mpsc::unbounded();

        Ok(Self {
            requests,
            host_responses,
            client_errors,
            send_half,
            error_sender,
            api,
            connecting: Some(rx),
        })
    }

    #[cfg(feature = "use-node")]
    fn sender_half(&self) -> WebApiRequestClient {
        WebApiRequestClient {
            sender: self.send_half.clone(),
            responses: self.error_sender.clone(),
        }
    }

    #[cfg(not(feature = "use-node"))]
    fn sender_half(&self) -> WebApiRequestClient {
        WebApiRequestClient
    }
}

#[cfg(feature = "use-node")]
#[derive(Clone, Debug)]
pub(crate) struct WebApiRequestClient {
    sender: ClientRequester,
    responses: NodeResponses,
}

#[cfg(not(feature = "use-node"))]
#[derive(Clone, Debug)]
pub(crate) struct WebApiRequestClient;

impl WebApiRequestClient {
    #[cfg(feature = "use-node")]
    pub async fn send(
        &mut self,
        request: locutus_stdlib::client_api::ClientRequest<'static>,
    ) -> Result<(), locutus_stdlib::client_api::Error> {
        self.sender
            .send(request)
            .await
            .map_err(|_| locutus_stdlib::client_api::Error::ChannelClosed)?;
        self.sender.flush().await.unwrap();
        Ok(())
    }

    #[cfg(not(feature = "use-node"))]
    pub async fn send(
        &mut self,
        request: locutus_stdlib::client_api::ClientRequest<'static>,
    ) -> Result<(), locutus_stdlib::client_api::Error> {
        tracing::debug!(?request, "emulated request");
        Ok(())
    }
}

#[cfg(feature = "use-node")]
impl From<WebApiRequestClient> for NodeResponses {
    fn from(val: WebApiRequestClient) -> Self {
        val.responses
    }
}

#[cfg(not(feature = "use-node"))]
impl From<WebApiRequestClient> for NodeResponses {
    fn from(_val: WebApiRequestClient) -> Self {
        unimplemented!()
    }
}

#[cfg(feature = "use-node")]
mod identity_management {
    use super::*;
    use ::identity_management::*;
    use locutus_stdlib::{client_api::DelegateRequest, prelude::*};

    const ID_MANAGER_CODE_HASH: &str =
        include_str!("../../../../modules/identity-management/build/identity_management_code_hash");
    const ID_MANAGER_CODE: &[u8] =
        include_bytes!("../../../../modules/identity-management/build/locutus/identity_management");
    const ID_MANAGER_KEY: &[u8] = include_bytes!("../build/identity-manager-params");

    fn identity_manager_key() -> Result<(DelegateKey, SecretsId, Parameters<'static>), DynError> {
        let params = IdentityParams::try_from(ID_MANAGER_KEY)?;
        let secret_id = params.as_secret_id();
        let params = params.try_into()?;
        let key = DelegateKey::from_params(ID_MANAGER_CODE_HASH, &params)?;
        Ok((key, secret_id, params))
    }

    pub(super) async fn create_delegate(client: &mut WebApiRequestClient) -> Result<(), DynError> {
        let (key, _, params) = identity_manager_key()?;
        let delegate = Delegate::from((&DelegateCode::from(ID_MANAGER_CODE), &params));
        assert_eq!(&key, delegate.key());
        let request = ClientRequest::DelegateOp(DelegateRequest::RegisterDelegate {
            delegate,
            cipher: DelegateRequest::DEFAULT_CIPHER,
            nonce: DelegateRequest::DEFAULT_NONCE,
        });
        crate::log::debug!("creating identity manager with key: {key}");
        client.send(request).await?;
        Ok(())
    }

    pub(super) async fn load_aliases(
        client: &mut WebApiRequestClient,
    ) -> Result<DelegateKey, DynError> {
        let (key, secret_id, params) = identity_manager_key()?;
        crate::log::debug!("loading aliases ({key})");
        let request = DelegateRequest::ApplicationMessages {
            params,
            inbound: vec![GetSecretRequest::new(secret_id).into()],
            key: key.clone(),
        };
        client.send(request.into()).await?;
        Ok(key)
    }

    pub(super) async fn create_alias(
        client: &mut WebApiRequestClient,
        alias: String,
        key: Vec<u8>,
        extra: String,
    ) -> Result<(), DynError> {
        crate::log::debug!("creating {alias}");
        let (delegate_key, _, params) = identity_manager_key()?;
        let msg = IdentityMsg::CreateIdentity {
            alias,
            key,
            extra: Some(extra),
        };
        let request = DelegateRequest::ApplicationMessages {
            params,
            inbound: vec![InboundDelegateMsg::ApplicationMessage(
                ApplicationMessage::new(ContractInstanceId::new([0; 32]), (&msg).try_into()?),
            )],
            key: delegate_key.clone(),
        };
        client.send(request.into()).await?;
        Ok(())
    }
}

#[cfg(feature = "use-node")]
pub(crate) async fn node_comms(
    mut rx: UnboundedReceiver<crate::app::NodeAction>,
    contracts: Vec<crate::app::Identity>,
    // todo: refactor: instead of passing this arround,
    // where necessary we could be getting the fresh data via static methods calls to Inbox
    // and store the information there in thread locals
    mut inboxes: crate::app::InboxesData,
) {
    use std::{rc::Rc, sync::Arc};

    use freenet_email_inbox::Inbox as StoredInbox;
    use futures::StreamExt;
    use locutus_stdlib::{
        client_api::{ContractError, ContractResponse, DelegateError, ErrorKind, RequestError},
        prelude::*,
    };

    use crate::{
        aft::AftRecords,
        app::{set_aliases, Identity, NodeAction},
        inbox::InboxModel,
    };

    let mut inbox_contract_to_id = HashMap::new();
    let mut token_contract_to_id = HashMap::new();
    // let mut id_to_token_contract = HashMap::new();
    let mut api = WebApi::new()
        .map_err(|err| {
            crate::log::error(format!("error while connecting to node: {err}"), None);
            err
        })
        .expect("open connection");
    api.connecting.take().unwrap().await.unwrap();
    let mut req_sender = api.sender_half();
    crate::inbox::InboxModel::load_all(&mut req_sender, &contracts, &mut inbox_contract_to_id)
        .await;
    crate::aft::AftRecords::load_all(&mut req_sender, &contracts, &mut token_contract_to_id).await;
    let identities_key = identity_management::load_aliases(&mut req_sender)
        .await
        .unwrap();
    WEB_API_SENDER.set(req_sender).unwrap();

    static IDENTITIES_KEY: OnceLock<DelegateKey> = OnceLock::new();
    IDENTITIES_KEY.set(identities_key.clone()).unwrap();

    async fn handle_action(
        req: NodeAction,
        api: &WebApi,
        waiting_updates: &mut HashMap<ContractKey, Identity>,
    ) {
        let mut client = api.sender_half();

        match req {
            NodeAction::LoadMessages(identity) => {
                match InboxModel::load(&mut client, &identity).await {
                    Err(err) => {
                        node_response_error_handling(
                            client.into(),
                            Err(err),
                            TryNodeAction::LoadInbox,
                        )
                        .await;
                    }
                    Ok(key) => {
                        waiting_updates.entry(key).or_insert(identity);
                    }
                }
            }
            NodeAction::LoadIdentities => {
                match identity_management::load_aliases(&mut client).await {
                    Ok(_) => {}
                    Err(e) => crate::log::error(format!("{e}"), Some(TryNodeAction::LoadAliases)),
                }
            }
            NodeAction::CreateIdentity {
                alias,
                key,
                description: extra,
            } => {
                match identity_management::create_alias(&mut client, alias.clone(), key, extra)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => crate::log::error(
                        format!("{e}"),
                        Some(TryNodeAction::CreateIdentity(alias)),
                    ),
                }
            }
        }
    }

    async fn handle_response(
        res: Result<HostResponse, ClientError>,
        inbox_to_id: &mut HashMap<ContractKey, Identity>,
        token_rec_to_id: &mut HashMap<ContractKey, Identity>,
        inboxes: &mut crate::app::InboxesData,
    ) {
        let mut client = WEB_API_SENDER.get().unwrap().clone();
        let res = match res {
            Ok(r) => r,
            Err(e) => {
                if let ErrorKind::RequestError(err) = e.kind() {
                    // FIXME: handle the different possible errors
                    match err {
                        RequestError::ContractError(ContractError::Update { key, .. }) => {
                            if token_rec_to_id.get(&key).is_some() {
                                // FIXME: in case this is for a token record which is PENDING_CONFIRMED_ASSIGNMENTS
                                // we should reject that pending assignment
                                // FIXME: in case this is for an inbox contract we were trying to update, this means
                                let id = token_rec_to_id.get(&key).unwrap();
                                let alias = id.alias();
                                crate::log::error(format!("the message for {alias} (aft contract: {key}) wasn't delivered successfully, so may need to try again and/or notify the user"), None);
                            } else if inbox_to_id.get(&key).is_some() {
                                let id = inbox_to_id.get(&key).unwrap();
                                let alias = id.alias();
                                crate::log::error(format!("the message for {alias} (inbox contract: {key}) wasn't delievered succesffully, so may need to try again and/or notify the user"), None);
                            }
                        }
                        RequestError::ContractError(err) => {
                            crate::log::error(format!("FIXME: {err}"), None)
                        }
                        RequestError::DelegateError(DelegateError::Missing(key))
                            if &key == IDENTITIES_KEY.get().unwrap() =>
                        {
                            if let Err(e) = identity_management::create_delegate(&mut client).await
                            {
                                crate::log::error(format!("{e}"), None);
                            }
                        }
                        RequestError::DelegateError(error) => {
                            crate::log::error(
                                format!("received delegate request error: {error}"),
                                None,
                            );
                        }
                        RequestError::Disconnect => {
                            todo!("lost connection to node, should retry connecting")
                        }
                    }
                }
                return;
            }
        };
        crate::log::debug!("got node response: {res}");
        match res {
            HostResponse::ContractResponse(ContractResponse::GetResponse {
                key, state, ..
            }) => {
                if let Some(identity) = inbox_to_id.remove(&key) {
                    // is an inbox contract
                    let state: StoredInbox = serde_json::from_slice(state.as_ref()).unwrap();
                    let updated_model =
                        InboxModel::from_state(identity.key.clone(), state, key.clone()).unwrap();
                    let loaded_models = inboxes.load();
                    if let Some(pos) = loaded_models.iter().position(|e| {
                        let x = e.borrow();
                        x.key == key
                    }) {
                        crate::log::debug!(
                            "loaded inbox {key} with {} messages",
                            updated_model.messages.len()
                        );
                        let mut current = (*loaded_models[pos]).borrow_mut();
                        *current = updated_model;
                    } else {
                        crate::log::debug!("updated inbox {key}");
                        let mut with_new = (***loaded_models).to_vec();
                        std::mem::drop(loaded_models);
                        with_new.push(Rc::new(RefCell::new(updated_model)));
                        {
                            let keys = with_new
                                .iter()
                                .map(|i| format!("{}", i.borrow().key))
                                .collect::<Vec<_>>()
                                .join(", ");
                            crate::log::debug!("loaded inboxes: {keys}");
                        }
                        inboxes.store(Arc::new(with_new));
                    }
                    inbox_to_id.insert(key, identity);
                } else if let Some(identity) = token_rec_to_id.remove(&key) {
                    crate::log::debug!("updating AFT record for {identity}");
                    // is a AFT record contract
                    if let Err(e) = AftRecords::set(identity.clone(), state.into()) {
                        crate::log::error(format!("error setting an AFT record: {e}"), None);
                    }
                    token_rec_to_id.insert(key, identity);
                } else {
                    unreachable!("tried to get wrong contract key: {key}")
                }
            }
            HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key,
                update,
            }) => {
                if let Some(identity) = inbox_to_id.remove(&key) {
                    match update {
                        UpdateData::Delta(delta) => {
                            let delta: StoredInbox =
                                serde_json::from_slice(delta.as_ref()).unwrap();
                            let updated_model =
                                InboxModel::from_state(identity.key.clone(), delta, key.clone())
                                    .unwrap();
                            let loaded_models = inboxes.load();
                            let mut found = false;
                            for inbox in loaded_models.as_slice() {
                                if inbox.clone().borrow().key == key {
                                    let mut inbox = (**inbox).borrow_mut();
                                    inbox.merge(updated_model);
                                    crate::log::debug!(
                                        "updated inbox {key} with {} messages",
                                        inbox.messages.len()
                                    );
                                    found = true;
                                    break;
                                }
                            }
                            assert!(found);
                            inbox_to_id.insert(key, identity);
                        }
                        UpdateData::State(state) => {
                            let delta: StoredInbox =
                                serde_json::from_slice(state.as_ref()).unwrap();
                            let updated_model =
                                InboxModel::from_state(identity.key.clone(), delta, key.clone())
                                    .unwrap();
                            let loaded_models = inboxes.load();
                            let mut found = false;
                            for inbox in loaded_models.as_slice() {
                                if inbox.clone().borrow().key == key {
                                    let mut inbox = (**inbox).borrow_mut();
                                    *inbox = updated_model;
                                    crate::log::debug!(
                                        "updated inbox {key} (whole state) with {} messages",
                                        inbox.messages.len()
                                    );
                                    found = true;
                                    break;
                                }
                            }
                            assert!(found);
                            inbox_to_id.insert(key, identity);
                        }
                        // UpdateData::StateAndDelta { .. } => {
                        //     crate::log::error("recieved update state delta", None);
                        // }
                        _ => unreachable!(),
                    }
                } else if let Some(identity) = token_rec_to_id.remove(&key) {
                    // is a AFT record contract
                    if let Err(e) = AftRecords::update_record(identity.clone(), update) {
                        crate::log::error(
                            format!("error updating an AFT record from delta: {e}"),
                            None,
                        );
                    }
                    token_rec_to_id.insert(key, identity);
                } else {
                    unreachable!("tried to get wrong contract key: {key}")
                }
            }
            HostResponse::ContractResponse(ContractResponse::UpdateResponse { key, summary }) => {
                if let Some(identity) = token_rec_to_id.remove(&key) {
                    let summary = TokenAllocationSummary::try_from(summary).unwrap();
                    AftRecords::confirm_allocation(&mut client, key.id(), summary)
                        .await
                        .unwrap();
                    token_rec_to_id.insert(key, identity.clone());
                }
            }
            HostResponse::DelegateResponse { key, values } => {
                for msg in values {
                    match msg {
                        locutus_stdlib::prelude::OutboundDelegateMsg::ApplicationMessage(msg) => {
                            let token = match TokenDelegateMessage::try_from(msg.payload.as_slice())
                            {
                                Ok(r) => r,
                                Err(e) => {
                                    crate::log::error(
                                        format!("error deserializing delegate msg: {e}"),
                                        None,
                                    );
                                    return;
                                }
                            };
                            match token {
                                TokenDelegateMessage::AllocatedToken { assignment, .. } => {
                                    let token_contract_key =
                                        ContractKey::from(assignment.token_record);
                                    if let Some(identity) =
                                        token_rec_to_id.remove(&token_contract_key)
                                    {
                                        if let Err(e) = AftRecords::allocated_assignment(
                                            &mut client,
                                            assignment,
                                        )
                                        .await
                                        {
                                            // todo: if a collision occurs, the operation should be retried until there are no more tokens available
                                            crate::log::error(
                                                format!(
                                                    "error registering the token assignment: {e}"
                                                ),
                                                None,
                                            );
                                        }
                                        token_rec_to_id.insert(token_contract_key, identity);
                                    } else {
                                        unreachable!("tried to get wrong contract key: {key}")
                                    }
                                }
                                TokenDelegateMessage::Failure(reason) => {
                                    // FIXME: this may mean a pending message waiting for a token has failed, and need to notify that in the UI
                                    crate::log::error(
                                        format!("token assignment failure: {reason}"),
                                        Some(TryNodeAction::SendMessage),
                                    )
                                }
                                TokenDelegateMessage::RequestNewToken(_) => unreachable!(),
                            }
                        }
                        OutboundDelegateMsg::GetSecretResponse(GetSecretResponse {
                            value: Some(payload),
                            ..
                        }) => {
                            if &key == IDENTITIES_KEY.get().unwrap() {
                                let manager = ::identity_management::IdentityManagement::try_from(
                                    payload.as_ref(),
                                )
                                .unwrap();
                                set_aliases(manager);
                            }
                        }
                        other => {
                            crate::log::error(
                                format!("received wrong delegate msg: {other:?}"),
                                None,
                            );
                        }
                    }
                }
            }
            HostResponse::Ok => {}
            other => {
                crate::log::error(format!("message not handled: {other:?}"), None);
            }
        }
    }

    loop {
        futures::select! {
            r = api.host_responses.next() => {
                let Some(res) = r else { panic!("async action ch closed") };
                handle_response(
                    res,
                    &mut inbox_contract_to_id,
                    &mut token_contract_to_id,
                    &mut inboxes,
                )
                .await;
            }
            req = rx.next() => {
                let Some(req) = req else { panic!("async action ch closed") };
                handle_action(req, &api, &mut inbox_contract_to_id).await;
            }
            req = api.requests.next() => {
                let Some(req) = req else { panic!("request ch closed") };
                crate::log::debug!("sending request to API: {req}");
                api.api.send(req).await.unwrap();
            }
            error = api.client_errors.next() => {
                match error {
                    Some(Err((msg, action))) => crate::log::error(format!("{msg}"), Some(action)),
                    Some(Ok(_)) => {}
                    None => panic!("error ch closed"),
                }
            }
        }
    }
}

pub(crate) type AsyncActionResult = Result<(), (DynError, TryNodeAction)>;

pub(crate) async fn node_response_error_handling(
    mut error_channel: NodeResponses,
    res: Result<(), DynError>,
    action: TryNodeAction,
) {
    if let Err(error) = res {
        crate::log::error(format!("{error}"), Some(action.clone()));
        error_channel
            .send(Err((error, action)))
            .await
            .expect("error channel closed");
    } else {
        error_channel
            .send(Ok(()))
            .await
            .expect("error channel closed");
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TryNodeAction {
    LoadInbox,
    LoadTokenRecord,
    SendMessage,
    RemoveMessages,
    GetAlias,
    LoadAliases,
    CreateIdentity(String),
}

impl std::fmt::Display for TryNodeAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryNodeAction::LoadInbox => write!(f, "loading messages"),
            TryNodeAction::LoadTokenRecord => write!(f, "loading token record"),
            TryNodeAction::SendMessage => write!(f, "sending message"),
            TryNodeAction::RemoveMessages => write!(f, "removing messages"),
            TryNodeAction::GetAlias => write!(f, "get alias"),
            TryNodeAction::LoadAliases => write!(f, "load aliases"),
            TryNodeAction::CreateIdentity(alias) => write!(f, "create alias {alias}"),
        }
    }
}
