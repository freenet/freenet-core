use std::time::Duration;

use crate::contract::{Contract, ContractKey, ContractValue};

#[async_trait::async_trait]
pub(crate) trait UserEventsProxy {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    async fn recv(&mut self) -> UserEvent;
}

#[derive(Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum UserEvent {
    /// Update or insert a new value in a contract corresponding with the provided key.
    Put {
        /// Value to upsert in the contract.
        value: ContractValue,
        // TODO: this should be Either<ContractKey, Contract>
        contract: Contract,
    },
    /// Fetch the current value from a contract corresponding to the provided key.
    Get {
        /// Key of the contract.
        key: ContractKey,
        /// If this flag is set then fetch also the contract itself.
        contract: bool,
    },
    /// Subscribe to teh changes in a given contract. Implicitly starts a get operation
    /// if the contract is not present yet.
    Subscribe { key: ContractKey },
    /// Shutdown the node
    Shutdown,
}

pub(crate) struct UserEventHandler;

#[async_trait::async_trait]
impl UserEventsProxy for UserEventHandler {
    async fn recv(&mut self) -> UserEvent {
        loop {
            tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use std::{collections::HashMap, time::Duration};

    use rand::{prelude::Rng, thread_rng};
    use tokio::sync::watch::Receiver;

    use super::*;
    use crate::node::{test_utils::EventId, PeerKey};

    pub(crate) struct MemoryEventsGen {
        id: PeerKey,
        signal: Receiver<(EventId, PeerKey)>,
        non_owned_contracts: Vec<ContractKey>,
        owned_contracts: Vec<(Contract, ContractValue)>,
        events_to_gen: HashMap<EventId, UserEvent>,
    }

    impl MemoryEventsGen {
        pub fn new(signal: Receiver<(EventId, PeerKey)>, id: PeerKey) -> Self {
            Self {
                signal,
                id,
                non_owned_contracts: Vec::new(),
                owned_contracts: Vec::new(),
                events_to_gen: HashMap::new(),
            }
        }

        /// Contracts that are available in the network to be requested.
        pub fn request_contracts(&mut self, contracts: impl IntoIterator<Item = ContractKey>) {
            self.non_owned_contracts.extend(contracts.into_iter())
        }

        /// Contracts that the user updates.
        pub fn has_contract(
            &mut self,
            contracts: impl IntoIterator<Item = (Contract, ContractValue)>,
        ) {
            self.owned_contracts.extend(contracts.into_iter())
        }

        /// Events that the user generate.
        pub fn generate_events(&mut self, events: impl IntoIterator<Item = (EventId, UserEvent)>) {
            self.events_to_gen.extend(events.into_iter())
        }

        fn generate_deterministic_event(&mut self, id: &EventId) -> Option<UserEvent> {
            self.events_to_gen.remove(id)
        }

        fn generate_rand_event(&mut self) -> UserEvent {
            let mut rng = thread_rng();
            loop {
                match rng.gen_range(0u8..3) {
                    0 if !self.owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.owned_contracts.len());
                        let (contract, value) = self.owned_contracts[contract_no].clone();
                        break UserEvent::Put { contract, value };
                    }
                    1 if !self.non_owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.non_owned_contracts.len());
                        let key = self.non_owned_contracts[contract_no];
                        break UserEvent::Get {
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
                        break UserEvent::Subscribe { key };
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
    impl UserEventsProxy for MemoryEventsGen {
        async fn recv(&mut self) -> UserEvent {
            loop {
                if self.signal.changed().await.is_ok() {
                    let (ev_id, pk) = *self.signal.borrow();
                    if pk == self.id {
                        return self
                            .generate_deterministic_event(&ev_id)
                            .expect("event not found");
                    }
                } else {
                    log::debug!("sender half of user event gen dropped");
                    // probably the process finished, wait for a bit and then kill the thread
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    panic!("finished orphan background thread");
                }
            }
        }
    }
}
