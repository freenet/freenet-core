use dioxus::prelude::{UnboundedReceiver, UnboundedSender};
use locutus_stdlib::client_api::{ClientError, ClientRequest, HostResponse};
use locutus_stdlib::prelude::UpdateData;

use crate::app::AsyncActionResult;

type ClientRequester = UnboundedSender<ClientRequest<'static>>;
type HostResponses = crossbeam::channel::Receiver<Result<HostResponse, ClientError>>;

pub(crate) type NodeResponses = UnboundedSender<AsyncActionResult>;

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
    mut inboxes: crate::app::InboxesData,
) {
    use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

    use crossbeam::channel::TryRecvError;
    use freenet_email_inbox::Inbox as StoredInbox;
    use futures::StreamExt;
    use locutus_stdlib::{client_api::ContractResponse, prelude::ContractKey};

    use crate::{
        app::{error_handling, Identity, NodeAction, TryNodeAction, WEB_API_SENDER},
        inbox::InboxModel,
    };

    let mut contract_to_id = HashMap::new();
    let mut api = WebApi::new()
        .map_err(|err| {
            crate::log::error(format!("error while connecting to node: {err}"), None);
            err
        })
        .expect("open connection");
    api.connecting.take().unwrap().await.unwrap();
    crate::app::Inbox::load_all(api.sender_half(), &contracts, &mut contract_to_id).await;
    crate::log::log("requested inboxes");
    WEB_API_SENDER.set(api.sender_half()).unwrap();

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
        contract_to_id: &mut HashMap<ContractKey, Identity>,
        inboxes: &mut crate::app::InboxesData,
    ) {
        let res = match res {
            Ok(r) => r,
            Err(e) => {
                crate::log::error(format!("received error: {e}"), None);
                return;
            }
        };
        match res {
            HostResponse::ContractResponse(ContractResponse::GetResponse {
                key, state, ..
            }) => {
                let state: StoredInbox = serde_json::from_slice(state.as_ref()).unwrap();
                let Some(identity) = contract_to_id.remove(&key) else { unreachable!("tried to get wrong contract key: {key}") };
                let updated_model =
                    InboxModel::from_state(identity.key.clone(), state, key.clone()).unwrap();
                let loaded_models = inboxes.load();
                if let Some(pos) = loaded_models.iter().position(|e| {
                    let x = e.borrow();
                    x.key == key
                }) {
                    crate::log::log(format!("loaded inbox {key}"));
                    let mut current = loaded_models[pos].borrow_mut();
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
                contract_to_id.insert(key, identity);
            }
            HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key,
                update,
            }) => {
                match update {
                    UpdateData::Delta(delta) => {
                        crate::log::log(format!("recieved update delta"));
                        let delta: StoredInbox = serde_json::from_slice(delta.as_ref()).unwrap();
                        let Some(identity) = contract_to_id.remove(&key) else { unreachable!("tried to get wrong contract key: {key}") };
                        let updated_model =
                            InboxModel::from_state(identity.key.clone(), delta, key.clone()).unwrap();
                        let loaded_models = inboxes.load();
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
                        contract_to_id.insert(key, identity);
                        // TODO: Update only desired inbox model with new messages
                        // with_new.iter().enumerate().for_each(|(pos, inbox)| {
                        //     let mut inbox_mut = inbox.clone().borrow_mut();
                        //     if inbox_mut.key == delta.key {
                        //         let updated_model =
                        //             InboxModel::from_delta(delta.key, &mut inbox_mut, delta).unwrap();
                        //         with_new.push(Rc::new(RefCell::new(updated_model)));
                        //     } else {
                        //         with_new.push(inbox.clone());
                        //     }
                        // });
                    },
                    UpdateData::State(state) => {
                        crate::log::log(format!("recieved update state"));
                        todo!()
                    }
                    UpdateData::StateAndDelta { state, delta} => {
                        crate::log::log(format!("recieved update state delta"));
                        todo!()
                    }
                    _ => unreachable!(),
                }
            }
            HostResponse::ContractResponse(ContractResponse::UpdateResponse { .. }) => {}
            _ => todo!(),
        }
    }

    loop {
        match api.host_responses.try_recv() {
            Ok(res) => {
                handle_response(res, &mut contract_to_id, &mut inboxes).await;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                panic!("response ch closed");
            }
        }
        futures::select! {
            req = rx.next() => {
                let Some(req) = req else { panic!("async action ch closed") };
                handle_action(req, &api, &mut contract_to_id).await;
            }
            req = api.requests.next() => {
                let Some(req) = req else { panic!("request ch closed") };
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
