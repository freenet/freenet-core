use futures::future::BoxFuture;
use locutus_runtime::ComponentKey;
use locutus_stdlib::client_api::ClientRequest;
use locutus_stdlib::client_api::{ClientError, HostResponse};
use std::fmt::Debug;
use std::fmt::Display;

use locutus_runtime::prelude::ContractKey;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

pub(crate) mod combinator;
#[cfg(feature = "websocket")]
pub(crate) mod websocket;

pub type BoxedClient = Box<dyn ClientEventsProxy + Send + Sync + 'static>;
pub type HostResult = Result<HostResponse, ClientError>;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ClientId(pub(crate) usize);

impl From<ClientId> for usize {
    fn from(val: ClientId) -> Self {
        val.0
    }
}

impl ClientId {
    pub const FIRST: Self = ClientId(0);

    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

type HostIncomingMsg = Result<OpenRequest<'static>, ClientError>;

#[non_exhaustive]
pub struct OpenRequest<'a> {
    pub id: ClientId,
    pub request: ClientRequest<'a>,
    pub notification_channel: Option<UnboundedSender<HostResult>>,
}

impl<'a> OpenRequest<'a> {
    pub fn into_owned(self) -> OpenRequest<'static> {
        OpenRequest {
            request: self.request.into_owned(),
            ..self
        }
    }

    pub fn new(id: ClientId, request: ClientRequest<'a>) -> Self {
        Self {
            id,
            request,
            notification_channel: None,
        }
    }

    pub fn with_notification(mut self, ch: UnboundedSender<HostResult>) -> Self {
        self.notification_channel = Some(ch);
        self
    }
}

pub trait ClientEventsProxy {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    fn recv(&mut self) -> BoxFuture<'_, HostIncomingMsg>;

    /// Sends a response from the host to the client application.
    fn send(
        &mut self,
        id: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<Result<(), ClientError>>;
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum RequestError {
    #[error(transparent)]
    ContractError(#[from] ContractError),
    #[error(transparent)]
    ComponentError(#[from] ComponentError),
    #[error("client disconnect")]
    Disconnect,
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum ComponentError {
    #[error("error while registering component: {0}")]
    RegisterError(ComponentKey),
    #[error("execution error, cause: {0}")]
    ExecutionError(String),
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum ContractError {
    #[error("failed to get contract {key}, reason: {cause}")]
    Get { key: ContractKey, cause: String },
    #[error("put error for contract {key}, reason: {cause}")]
    Put { key: ContractKey, cause: String },
    #[error("update error for contract {key}, reason: {cause}")]
    Update { key: ContractKey, cause: String },
}

#[cfg(test)]
pub(crate) mod test {
    #![allow(unused)]

    // FIXME: remove unused
    use std::collections::HashMap;
    use std::sync::Arc;

    use futures::FutureExt;
    use locutus_runtime::{
        ContractCode, ContractContainer, Parameters, RelatedContracts, TryFromTsStd, WasmAPIVersion,
    };
    use locutus_stdlib::client_api::ContractRequest;
    use rand::{prelude::Rng, thread_rng};
    use tokio::sync::watch::Receiver;

    use crate::node::{test::EventId, PeerKey};
    use crate::{WrappedContract, WrappedState};

    use super::*;

    pub(crate) struct MemoryEventsGen {
        id: PeerKey,
        signal: Receiver<(EventId, PeerKey)>,
        non_owned_contracts: Vec<ContractKey>,
        owned_contracts: Vec<(ContractContainer, WrappedState)>,
        events_to_gen: HashMap<EventId, ClientRequest<'static>>,
        random: bool,
    }

    impl MemoryEventsGen {
        pub fn new(signal: Receiver<(EventId, PeerKey)>, id: PeerKey) -> Self {
            Self {
                signal,
                id,
                non_owned_contracts: Vec::new(),
                owned_contracts: Vec::new(),
                events_to_gen: HashMap::new(),
                random: false,
            }
        }

        /// Contracts that are available in the network to be requested.
        pub fn request_contracts(&mut self, contracts: impl IntoIterator<Item = ContractKey>) {
            self.non_owned_contracts.extend(contracts.into_iter())
        }

        /// Contracts that the user updates.
        pub fn has_contract(
            &mut self,
            contracts: impl IntoIterator<Item = (ContractContainer, WrappedState)>,
        ) {
            self.owned_contracts.extend(contracts);
        }

        /// Events that the user generate.
        pub fn generate_events(
            &mut self,
            events: impl IntoIterator<Item = (EventId, ClientRequest<'static>)>,
        ) {
            self.events_to_gen.extend(events.into_iter())
        }

        fn generate_deterministic_event(&mut self, id: &EventId) -> Option<ClientRequest> {
            self.events_to_gen.remove(id)
        }

        fn generate_rand_event(&mut self) -> ClientRequest<'static> {
            let mut rng = thread_rng();
            loop {
                match rng.gen_range(0u8..3) {
                    0 if !self.owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.owned_contracts.len());
                        let (contract, state) = self.owned_contracts[contract_no].clone();
                        break ContractRequest::Put {
                            contract,
                            state,
                            related_contracts: Default::default(),
                        }
                        .into();
                    }
                    1 if !self.non_owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.non_owned_contracts.len());
                        let key = self.non_owned_contracts[contract_no].clone();
                        break ContractRequest::Get {
                            key,
                            fetch_contract: rng.gen_bool(0.5),
                        }
                        .into();
                    }
                    2 if !self.non_owned_contracts.is_empty()
                        || !self.owned_contracts.is_empty() =>
                    {
                        let get_owned = match (
                            self.non_owned_contracts.is_empty(),
                            self.owned_contracts.is_empty(),
                        ) {
                            (false, false) => rng.gen_bool(0.5),
                            (false, true) => false,
                            (true, false) => true,
                            _ => unreachable!(),
                        };
                        let key = if get_owned {
                            let contract_no = rng.gen_range(0..self.owned_contracts.len());
                            self.owned_contracts[contract_no].0.clone().key()
                        } else {
                            // let contract_no = rng.gen_range(0..self.non_owned_contracts.len());
                            // self.non_owned_contracts[contract_no]
                            todo!("fixme")
                        };
                        break ContractRequest::Subscribe { key }.into();
                    }
                    0 => {}
                    1 => {}
                    2 => {
                        let msg = "the joint set of owned and non-owned contracts is empty!";
                        log::error!("{}", msg);
                        panic!("{}", msg)
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    impl ClientEventsProxy for MemoryEventsGen {
        fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
            // async move {
            //     loop {
            //         if self.signal.changed().await.is_ok() {
            //             let (ev_id, pk) = *self.signal.borrow();
            //             if pk == self.id && !self.random {
            //                 let res = OpenRequest {
            //                     id: ClientId(1),
            //                     request: self
            //                         .generate_deterministic_event(&ev_id)
            //                         .expect("event not found"),
            //                     notification_channel: None,
            //                 };
            //                 return Ok(res);
            //             } else if pk == self.id {
            //                 let res = OpenRequest {
            //                     id: ClientId(1),
            //                     request: self.generate_rand_event(),
            //                     notification_channel: None,
            //                 };
            //                 return Ok(res);
            //             }
            //         } else {
            //             log::debug!("sender half of user event gen dropped");
            //             // probably the process finished, wait for a bit and then kill the thread
            //             tokio::time::sleep(Duration::from_secs(1)).await;
            //             panic!("finished orphan background thread");
            //         }
            //     }
            // }
            // .boxed()
            todo!("fixme")
        }

        fn send(
            &mut self,
            _id: ClientId,
            _response: Result<HostResponse, ClientError>,
        ) -> BoxFuture<'_, Result<(), ClientError>> {
            async { Ok(()) }.boxed()
        }
    }

    // #[test]
    // fn put_response_serialization() -> Result<(), Box<dyn std::error::Error>> {
    //     let bytes = crate::util::test::random_bytes_1024();
    //     let mut gen = arbitrary::Unstructured::new(&bytes);

    //     let key = ContractKey::from((
    //         &gen.arbitrary::<Parameters>()?,
    //         &gen.arbitrary::<ContractCode>()?,
    //     ));
    //     let complete_put: HostResult = Ok(HostResponse::PutResponse { key });
    //     let encoded = rmp_serde::to_vec(&complete_put)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;

    //     let key = ContractKey::from_id(key.encoded_contract_id())?;
    //     let only_spec: HostResult = Ok(HostResponse::PutResponse { key });
    //     let encoded = rmp_serde::to_vec(&only_spec)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
    //     Ok(())
    // }

    // #[test]
    // fn update_response_serialization() -> Result<(), Box<dyn std::error::Error>> {
    //     let bytes = crate::util::test::random_bytes_1024();
    //     let mut gen = arbitrary::Unstructured::new(&bytes);

    //     let update: HostResult = Ok(HostResponse::UpdateResponse {
    //         key: gen.arbitrary()?,
    //         summary: gen.arbitrary()?,
    //     });
    //     let encoded = rmp_serde::to_vec(&update)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
    //     Ok(())
    // }

    // #[test]
    // fn get_response_serialization() -> Result<(), Box<dyn std::error::Error>> {
    //     let bytes = crate::util::test::random_bytes_1024();
    //     let mut gen = arbitrary::Unstructured::new(&bytes);

    //     let state: WrappedState = gen.arbitrary()?;
    //     let complete_get: HostResult = Ok(HostResponse::GetResponse {
    //         contract: Some(gen.arbitrary()?),
    //         state: state.clone(),
    //     });
    //     let encoded = rmp_serde::to_vec(&complete_get)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;

    //     let incomplete_get: HostResult = Ok(HostResponse::GetResponse {
    //         contract: None,
    //         state,
    //     });
    //     let encoded = rmp_serde::to_vec(&incomplete_get)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
    //     Ok(())
    // }

    // #[test]
    // fn update_notification_serialization() -> Result<(), Box<dyn std::error::Error>> {
    //     let bytes = crate::util::test::random_bytes_1024();
    //     let mut gen = arbitrary::Unstructured::new(&bytes);

    //     let update_notif: HostResult = Ok(HostResponse::UpdateNotification {
    //         key: gen.arbitrary()?,
    //         update: StateDelta::from(gen.arbitrary::<Vec<u8>>()?).into(),
    //     });
    //     let encoded = rmp_serde::to_vec(&update_notif)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
    //     Ok(())
    // }

    #[test]
    fn test_handle_update_request() -> Result<(), Box<dyn std::error::Error>> {
        let expected_client_request: ContractRequest = ContractRequest::Update {
            key: ContractKey::from_id("DCBi7HNZC3QUZRiZLFZDiEduv5KHgZfgBk8WwTiheGq1".to_string())
                .unwrap(),
            data: locutus_runtime::StateDelta::from(vec![0, 1, 2]).into(),
        };
        let msg: Vec<u8> = vec![
            130, 163, 107, 101, 121, 130, 168, 105, 110, 115, 116, 97, 110, 99, 101, 196, 32, 181,
            41, 189, 142, 103, 137, 251, 46, 133, 213, 21, 255, 179, 17, 3, 17, 240, 208, 191, 5,
            215, 72, 60, 41, 194, 14, 217, 228, 225, 251, 209, 100, 164, 99, 111, 100, 101, 192,
            164, 100, 97, 116, 97, 129, 165, 100, 101, 108, 116, 97, 196, 3, 0, 1, 2,
        ];

        let result_client_request: ContractRequest = ContractRequest::try_decode(&msg)?;
        assert_eq!(result_client_request, expected_client_request);
        Ok(())
    }

    #[test]
    fn test_handle_get_request() -> Result<(), Box<dyn std::error::Error>> {
        let expected_client_request: ContractRequest = ContractRequest::Get {
            key: ContractKey::from_id("JAgVrRHt88YbBFjGQtBD3uEmRUFvZQqK7k8ypnJ8g6TC".to_string())
                .unwrap(),
            fetch_contract: false,
        };
        let msg: Vec<u8> = vec![
            130, 163, 107, 101, 121, 130, 168, 105, 110, 115, 116, 97, 110, 99, 101, 196, 32, 255,
            17, 144, 159, 194, 187, 46, 33, 205, 77, 242, 70, 87, 18, 202, 62, 226, 149, 25, 151,
            188, 167, 153, 197, 129, 25, 179, 198, 218, 99, 159, 139, 164, 99, 111, 100, 101, 192,
            173, 102, 101, 116, 99, 104, 67, 111, 110, 116, 114, 97, 99, 116, 194,
        ];

        let result_client_request: ContractRequest = ContractRequest::try_decode(&msg)?;
        assert_eq!(result_client_request, expected_client_request);
        Ok(())
    }

    #[test]
    fn test_handle_put_request() -> Result<(), Box<dyn std::error::Error>> {
        let expected_client_request: ContractRequest = ContractRequest::Put {
            contract: ContractContainer::Wasm(WasmAPIVersion::V1(WrappedContract::new(
                Arc::new(ContractCode::from(vec![1])),
                Parameters::from(vec![2]),
            ))),
            state: WrappedState::new(vec![3]),
            related_contracts: RelatedContracts::from(HashMap::new()),
        };

        let msg: Vec<u8> = vec![
            131, 169, 99, 111, 110, 116, 97, 105, 110, 101, 114, 132, 163, 107, 101, 121, 130, 168,
            105, 110, 115, 116, 97, 110, 99, 101, 196, 32, 135, 191, 81, 248, 20, 212, 21, 0, 62,
            82, 40, 7, 217, 52, 41, 65, 33, 245, 251, 131, 23, 147, 59, 124, 134, 129, 218, 158,
            113, 132, 235, 85, 164, 99, 111, 100, 101, 192, 164, 100, 97, 116, 97, 196, 1, 1, 170,
            112, 97, 114, 97, 109, 101, 116, 101, 114, 115, 196, 1, 2, 167, 118, 101, 114, 115,
            105, 111, 110, 162, 86, 49, 165, 115, 116, 97, 116, 101, 196, 1, 3, 176, 114, 101, 108,
            97, 116, 101, 100, 67, 111, 110, 116, 114, 97, 99, 116, 115, 128,
        ];

        let result_client_request: ContractRequest = ContractRequest::try_decode(&msg)?;
        assert_eq!(result_client_request, expected_client_request);
        Ok(())
    }
}
