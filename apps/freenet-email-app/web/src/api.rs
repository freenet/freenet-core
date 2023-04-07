use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use dioxus::prelude::{UnboundedReceiver, UnboundedSender};
use locutus_stdlib::client_api::{ClientError, ClientRequest, HostResponse};

use crate::app::AsyncActionResult;

type ClientRequester = UnboundedSender<ClientRequest<'static>>;
type HostResponses = crossbeam::channel::Receiver<Result<HostResponse, ClientError>>;

pub(crate) type NodeResponses = crossbeam::channel::Sender<AsyncActionResult>;

#[cfg(feature = "use-node")]
struct WebApi {
    requests: UnboundedReceiver<ClientRequest<'static>>,
    host_responses: HostResponses,
    client_errors: crossbeam::channel::Receiver<AsyncActionResult>,
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
        use futures::StreamExt;
        use wasm_bindgen::JsCast;
        let conn = web_sys::WebSocket::new("ws://localhost:50509/contract/command/").unwrap();
        let (send_host_responses, host_responses) = crossbeam::channel::unbounded();
        let (send_half, requests) = futures::channel::mpsc::unbounded();
        let result_handler = move |result: Result<HostResponse, ClientError>| {
            send_host_responses.send(result).expect("channel open");
        };
        let (tx, rx) = futures::channel::oneshot::channel();
        let onopen_handler = move || {
            tx.send(());
            crate::log::log("Connected to websocket");
        };
        let mut api = locutus_stdlib::client_api::WebApi::start(
            conn,
            result_handler,
            |err| {
                crate::log::error(format!("connection error: {err}"), None);
            },
            onopen_handler,
        );
        let (error_sender, client_errors) = crossbeam::channel::unbounded();

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
    mut rx: UnboundedReceiver<crate::app::AsyncAction>,
    contracts: Vec<crate::app::Identity>,
    inboxes: crate::app::InboxesData,
) {
    use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};

    use crossbeam::channel::TryRecvError;
    use freenet_email_inbox::Inbox as StoredInbox;
    use futures::StreamExt;
    use locutus_stdlib::client_api::ContractResponse;

    use crate::{
        app::{error_handling, AsyncAction, TryAsyncAction, WEB_API_SENDER},
        inbox::InboxModel,
    };

    let mut api = WebApi::new()
        .map_err(|err| {
            crate::log::error(format!("error while connecting to node: {err}"), None);
            err
        })
        .expect("open connection");
    api.connecting.take().unwrap().await.unwrap();
    crate::app::Inbox::load_all(api.sender_half(), &contracts).await;
    crate::log::log("Loaded inbox");
    WEB_API_SENDER.set(api.sender_half()).unwrap();

    let mut waiting_updates = HashMap::new();
    async move {
        loop {
            while let Ok(Some(req)) = rx.try_next() {
                let AsyncAction::LoadMessages(identity) = req;
                let mut client = api.sender_half();
                match InboxModel::load(&mut client, &identity).await {
                    Err(err) => {
                        error_handling(client.into(), Err(err), TryAsyncAction::LoadMessages).await;
                    }
                    Ok(key) => {
                        waiting_updates.entry(key).or_insert(identity);
                    }
                }
            }
            while let Ok(Some(req)) = api.requests.try_next() {
                api.api.send(req).await.unwrap();
            }

            match api.client_errors.try_recv() {
                Err(TryRecvError::Empty) | Ok(Ok(())) => {}
                Err(TryRecvError::Disconnected) => panic!(),
                Ok(Err((err, _action))) => {
                    eprintln!("{err}");
                    todo!("better error handling");
                }
            }

            match api.host_responses.try_recv() {
                Ok(r) => {
                    let r = r.unwrap();
                    match r {
                        HostResponse::ContractResponse(ContractResponse::GetResponse {
                            key,
                            state,
                            ..
                        }) => {
                            let state: StoredInbox =
                                serde_json::from_slice(state.as_ref()).unwrap();
                            let Some(identity) = waiting_updates.remove(&key) else { unreachable!() };
                            let updated_model =
                                InboxModel::from_state(identity.key.clone(), state, key.clone())
                                    .unwrap();
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
                                let mut cloned = (***loaded_models).to_vec();
                                std::mem::drop(loaded_models);
                                cloned.push(Rc::new(RefCell::new(updated_model)));
                            }
                        }
                        _ => panic!(),
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => panic!(),
            }
            #[cfg(target_family = "wasm")]
            {
                web_sys::window()
                    .unwrap()
                    .set_timeout_with_str_and_timeout_and_unused_0("wait_msg", 10);
            }
            #[cfg(not(target_family = "wasm"))]
            {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    };
}
