use std::fmt::Debug;
use std::{error::Error as StdError, fmt::Display};

use either::Either;
use locutus_runtime::prelude::*;
use locutus_stdlib::prelude::{State, StateDelta};
use serde::{Deserialize, Serialize};

use crate::Contract;

pub(crate) mod combinator;
#[cfg(feature = "websocket")]
pub(crate) mod websocket;

pub type BoxedClient = Box<dyn ClientEventsProxy + Send + Sync + 'static>;
type HostResult = Result<HostResponse, ClientError>;

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ClientId(pub(crate) usize);

impl ClientId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientError {
    kind: ErrorKind,
}

impl ClientError {
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl From<ErrorKind> for ClientError {
    fn from(kind: ErrorKind) -> Self {
        ClientError { kind }
    }
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub enum ErrorKind {
    #[error("comm channel between client/host closed")]
    ChannelClosed,
    #[error("lost the connection with the protocol hanling connections")]
    TransportProtocolDisconnect,
    #[error("unknown client id: {0}")]
    UnknownClient(ClientId),
}

impl From<ErrorKind> for warp::Rejection {
    fn from(_: ErrorKind) -> Self {
        todo!()
    }
}

impl Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientError")
    }
}

impl StdError for ClientError {}

// fixme: only use this internally, communication outside of the node should come from
//        one of the offered client interfaces enabled through features (eg. websockets)
#[async_trait::async_trait]
pub trait ClientEventsProxy {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    async fn recv(&mut self) -> Result<(ClientId, ClientRequest), ClientError>;

    /// Sends a response from the host to the client application.
    async fn send(
        &mut self,
        id: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> Result<(), ClientError>;

    fn cloned(&self) -> BoxedClient;
}

/// A response to a previous [`ClientRequest`]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum HostResponse {
    PutResponse(ContractKey),
    /// Successful update
    UpdateResponse(ContractKey),
    /// Message sent when there is an update to a subscribed command.
    UpdateNotification {
        key: ContractKey,
        update: Either<StateDelta<'static>, State<'static>>,
    },
    GetResponse {
        contract: Option<Contract>,
        state: Option<State<'static>>,
    },
}

/// A request from a client application to the host.
#[derive(Clone, Serialize, Deserialize, Debug)]
// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum ClientRequest {
    /// Insert a new value in a contract corresponding with the provided key.
    Put {
        contract: Contract,
        /// Value to upsert in the contract.
        state: ContractState,
    },
    /// Update an existing contract corresponding with the provided key.
    Update {
        key: ContractKey,
        delta: Either<StateDelta<'static>, State<'static>>,
    },
    /// Fetch the current value from a contract corresponding to the provided key.
    Get {
        /// Key of the contract.
        key: ContractKey,
        /// If this flag is set then fetch also the contract itself.
        contract: bool,
    },
    /// Subscribe to the changes in a given contract. Implicitly starts a get operation
    /// if the contract is not present yet.
    /// After this action the client will start receiving all changes though the open
    /// connection.
    Subscribe {
        key: ContractKey,
    },
    Disconnect {
        cause: Option<String>,
    },
}

#[cfg(test)]
pub(crate) mod test {
    use std::{collections::HashMap, time::Duration};

    use rand::{prelude::Rng, thread_rng};
    use tokio::sync::watch::Receiver;

    use crate::node::{test::EventId, PeerKey};

    use super::*;

    pub(crate) struct MemoryEventsGen {
        id: PeerKey,
        signal: Receiver<(EventId, PeerKey)>,
        non_owned_contracts: Vec<ContractKey>,
        owned_contracts: Vec<(Contract, ContractState)>,
        events_to_gen: HashMap<EventId, ClientRequest>,
        random: bool,
    }

    impl MemoryEventsGen {
        pub fn new_tmp() -> Self {
            todo!()
        }

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
            contracts: impl IntoIterator<Item = (Contract, ContractState)>,
        ) {
            self.owned_contracts.extend(contracts);
        }

        /// Events that the user generate.
        pub fn generate_events(
            &mut self,
            events: impl IntoIterator<Item = (EventId, ClientRequest)>,
        ) {
            self.events_to_gen.extend(events.into_iter())
        }

        fn generate_deterministic_event(&mut self, id: &EventId) -> Option<ClientRequest> {
            self.events_to_gen.remove(id)
        }

        fn generate_rand_event(&mut self) -> ClientRequest {
            let mut rng = thread_rng();
            loop {
                match rng.gen_range(0u8..3) {
                    0 if !self.owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.owned_contracts.len());
                        let (contract, value) = self.owned_contracts[contract_no].clone();
                        break ClientRequest::Put {
                            contract,
                            state: value,
                        };
                    }
                    1 if !self.non_owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.non_owned_contracts.len());
                        let key = self.non_owned_contracts[contract_no];
                        break ClientRequest::Get {
                            key,
                            contract: rng.gen_bool(0.5),
                        };
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
                            self.owned_contracts[contract_no].0.key()
                        } else {
                            let contract_no = rng.gen_range(0..self.non_owned_contracts.len());

                            self.non_owned_contracts[contract_no]
                        };
                        break ClientRequest::Subscribe { key };
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

    #[async_trait::async_trait]
    impl ClientEventsProxy for MemoryEventsGen {
        async fn recv(&mut self) -> Result<(ClientId, ClientRequest), ClientError> {
            loop {
                if self.signal.changed().await.is_ok() {
                    let (ev_id, pk) = *self.signal.borrow();
                    if pk == self.id && !self.random {
                        return Ok((
                            ClientId(1),
                            self.generate_deterministic_event(&ev_id)
                                .expect("event not found"),
                        ));
                    } else if pk == self.id {
                        return Ok((ClientId(1), self.generate_rand_event()));
                    }
                } else {
                    log::debug!("sender half of user event gen dropped");
                    // probably the process finished, wait for a bit and then kill the thread
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    panic!("finished orphan background thread");
                }
            }
        }

        async fn send(
            &mut self,
            _id: ClientId,
            _response: Result<HostResponse, ClientError>,
        ) -> Result<(), ClientError> {
            todo!()
        }

        fn cloned(&self) -> BoxedClient {
            todo!()
        }
    }
}
