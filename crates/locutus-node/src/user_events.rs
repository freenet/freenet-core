use crate::contract::{Contract, ContractKey, ContractValue};

#[async_trait::async_trait]
pub(crate) trait UserEventsProxy {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    async fn recv(&mut self) -> UserEvent;
}

// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum UserEvent {
    /// Update or insert a new value in a contract corresponding with the provided key.
    Put {
        /// Value to upsert in the contract.
        value: ContractValue,
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
}

pub(crate) mod test_utils {
    use std::time::Duration;

    use arbitrary::{Arbitrary, Unstructured};
    use rand::{prelude::Rng, thread_rng};
    use tokio::sync::watch::Receiver;

    use super::*;
    use crate::{conn_manager::PeerKey, test_utils::random_bytes_128};

    pub(crate) struct MemoryEventsGen {
        signal: Receiver<PeerKey>,
        id: PeerKey,
        non_owned_contracts: Vec<ContractKey>,
        owned_contracts: Vec<Contract>,
    }

    impl MemoryEventsGen {
        pub fn new(signal: Receiver<PeerKey>, id: PeerKey) -> Self {
            Self {
                signal,
                id,
                non_owned_contracts: Vec::new(),
                owned_contracts: Vec::new(),
            }
        }

        /// Contracts that are available in the network to be requested.
        pub fn request_contracts(&mut self, contracts: impl IntoIterator<Item = ContractKey>) {
            self.non_owned_contracts.extend(contracts.into_iter())
        }

        /// Contracts that the user updates.
        pub fn has_contract(&mut self, contracts: impl IntoIterator<Item = Contract>) {
            self.owned_contracts.extend(contracts.into_iter())
        }

        fn gen_new_event(&mut self) -> UserEvent {
            let mut rng = thread_rng();
            loop {
                match rng.gen_range(0u8..3) {
                    0 if !self.owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.owned_contracts.len());
                        let contract = self.owned_contracts[contract_no].clone();

                        let bytes = random_bytes_128();
                        let mut unst = Unstructured::new(&bytes);
                        let value =
                            ContractValue::arbitrary(&mut unst).expect("failed gen arb data");

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
                            self.owned_contracts[contract_no].key()
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
            while self.signal.changed().await.is_ok() {
                if *self.signal.borrow() == self.id {
                    return self.gen_new_event();
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            panic!()
        }
    }
}
