use std::{cell::RefCell, collections::HashMap};

use dioxus::prelude::{UnboundedReceiver, UnboundedSender};
use locutus_aft_interface::{TokenAllocationSummary, TokenDelegateMessage};
use locutus_stdlib::client_api::{ClientError, ClientRequest, HostResponse};
use locutus_stdlib::prelude::UpdateData;
use once_cell::sync::OnceCell;

use crate::app::AsyncActionResult;

type ClientRequester = UnboundedSender<ClientRequest<'static>>;
type HostResponses = crossbeam::channel::Receiver<Result<HostResponse, ClientError>>;

pub(crate) type NodeResponses = UnboundedSender<AsyncActionResult>;

pub(crate) static WEB_API_SENDER: OnceCell<WebApiRequestClient> = OnceCell::new();

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
        let (send_host_responses, host_responses) = crossbeam::channel::unbounded();
        let (send_half, requests) = futures::channel::mpsc::unbounded();
        let result_handler = move |result: Result<HostResponse, ClientError>| {
            send_host_responses.send(result).expect("channel open");
        };
        let (tx, rx) = futures::channel::oneshot::channel();
        let onopen_handler = move || {
            tx.send(());
            crate::log::log("connected to websocket");
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
        use futures::SinkExt;
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
pub(crate) async fn node_comms(
    mut rx: UnboundedReceiver<crate::app::NodeAction>,
    contracts: Vec<crate::app::Identity>,
    // todo: refactor: instead of passing this arround,
    // where necessary we could be gettign thef resh data via static methods calls to InboxModel
    // and store the information there in thread locals
    mut inboxes: crate::app::InboxesData,
) {
    use std::{rc::Rc, sync::Arc};

    use crossbeam::channel::TryRecvError;
    use freenet_email_inbox::Inbox as StoredInbox;
    use futures::StreamExt;
    use locutus_stdlib::{
        client_api::{ContractError, ContractResponse, ErrorKind, RequestError},
        prelude::ContractKey,
    };

    use crate::{
        aft::AftRecords,
        app::{error_handling, Identity, NodeAction, TryNodeAction},
        inbox::InboxModel,
    };

    let mut inbox_contract_to_id = HashMap::new();
    let mut token_contract_to_id = HashMap::new();
    let mut id_to_token_contract = HashMap::new();
    let mut api = WebApi::new()
        .map_err(|err| {
            crate::log::error(format!("error while connecting to node: {err}"), None);
            err
        })
        .expect("open connection");
    api.connecting.take().unwrap().await.unwrap();
    let mut req_sender = api.sender_half();
    crate::inbox::InboxModel::load_all(&mut req_sender, &contracts, &mut inbox_contract_to_id, &mut id_to_token_contract)
        .await;
    crate::aft::AftRecords::load_all(&mut req_sender, &contracts, &mut token_contract_to_id).await;
    crate::log::log("requested inboxes");
    WEB_API_SENDER.set(req_sender).unwrap();

    async fn handle_action(
        req: NodeAction,
        api: &WebApi,
        waiting_updates: &mut HashMap<ContractKey, Identity>,
    ) {
        let NodeAction::LoadMessages(identity) = req;
        let mut client = api.sender_half();
        match InboxModel::load(&mut client, &identity).await {
            Err(err) => {
                error_handling(client.into(), Err(err), TryNodeAction::LoadInbox).await;
            }
            Ok(key) => {
                waiting_updates.entry(key).or_insert(identity);
            }
        }
    }

    async fn handle_response(
        res: Result<HostResponse, ClientError>,
        inbox_to_id: &mut HashMap<ContractKey, Identity>,
        token_rec_to_id: &mut HashMap<ContractKey, Identity>,
        id_to_token_contract: &mut HashMap<Identity, ContractKey>,
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
                            // FIXME: in case this is for a token record which is PENDING_CONFIRMED_ASSIGNMENTS
                            // we should reject that pending assignment

                            // FIXME: in case this is for an inbox contract we were trying to update, this means
                            // the message wasn't delivered successfully, so may need to try again and/or notify the user
                            todo!()
                        }
                        RequestError::ContractError(_) => todo!(),
                        RequestError::DelegateError(_) => todo!(),
                        RequestError::Disconnect => todo!(),
                    }
                }
                crate::log::error(format!("received error: {e}"), None);
                return;
            }
        };
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
                        crate::log::log(format!(
                            "loaded inbox {key} with {} messages",
                            updated_model.messages.len()
                        ));
                        let mut current = (*loaded_models[pos]).borrow_mut();
                        *current = updated_model;
                    } else {
                        crate::log::log(format!("updated inbox {key}"));
                        let mut with_new = (***loaded_models).to_vec();
                        std::mem::drop(loaded_models);
                        with_new.push(Rc::new(RefCell::new(updated_model)));
                        {
                            let keys = with_new
                                .iter()
                                .map(|i| format!("{}", i.borrow().key))
                                .collect::<Vec<_>>()
                                .join(", ");
                            crate::log::log(format!("loaded inboxes: {keys}"));
                        }
                        inboxes.store(Arc::new(with_new));
                    }
                    inbox_to_id.insert(key, identity);
                } else if let Some(identity) = token_rec_to_id.remove(&key) {
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
                                    crate::log::log(format!(
                                        "updated inbox {key} with {} messages",
                                        inbox.messages.len()
                                    ));
                                    found = true;
                                    break;
                                }
                            }
                            assert!(found);
                            inbox_to_id.insert(key, identity);
                        }
                        // UpdateData::State(_) => {
                        //     crate::log::error("recieved update state", None);
                        // }
                        // UpdateData::StateAndDelta { .. } => {
                        //     crate::log::error("recieved update state delta", None);
                        // }
                        _ => unreachable!(),
                    }
                } else if let Some(identity) = token_rec_to_id.remove(&key) {
                    // is a AFT record contract
                    match update {
                        UpdateData::Delta(delta) => {
                            if let Err(e) = AftRecords::update_record(identity.clone(), delta) {
                                crate::log::error(
                                    format!("error updating an AFT record: {e}"),
                                    None,
                                );
                            }
                            token_rec_to_id.insert(key, identity);
                        }
                        _ => unreachable!(),
                    }
                } else {
                    unreachable!("tried to get wrong contract key: {key}")
                }
            }
            HostResponse::ContractResponse(ContractResponse::UpdateResponse { key, summary }) => {
                if let Some(identity) = token_rec_to_id.remove(&key) {
                    if let Some(inbox_contract) = id_to_token_contract.remove(&identity){
                        let summary = TokenAllocationSummary::try_from(summary).unwrap();
                        AftRecords::confirm_allocation(&mut client, key.id(), summary, inbox_contract)
                            .await
                            .unwrap();
                        token_rec_to_id.insert(key.clone(), identity.clone());
                        id_to_token_contract.insert(identity, key);
                    }
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
                                            key.clone(),
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
                                        format!("{reason}"),
                                        Some(TryNodeAction::SendMessage),
                                    )
                                }
                                TokenDelegateMessage::RequestNewToken(_) => unreachable!(),
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
            _ => todo!(),
        }
    }

    loop {
        match api.host_responses.try_recv() {
            Ok(res) => {
                handle_response(
                    res,
                    &mut inbox_contract_to_id,
                    &mut token_contract_to_id,
                    &mut id_to_token_contract,
                    &mut inboxes,
                )
                .await;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                panic!("response ch closed");
            }
        }
        futures::select! {
            req = rx.next() => {
                let Some(req) = req else { panic!("async action ch closed") };
                handle_action(req, &api, &mut inbox_contract_to_id).await;
            }
            req = api.requests.next() => {
                let Some(req) = req else { panic!("request ch closed") };
                crate::log::debug(format!("sending request to API: {req:?}"));
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
