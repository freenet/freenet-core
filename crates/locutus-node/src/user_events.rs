use crate::contract::Contract;

#[async_trait::async_trait]
pub(crate) trait UserEventsProxy {
    async fn recv(&self) -> UserEvent;
}

#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum UserEvent {
    /// Update or insert a new value in a contract corresponding with the provided key.
    Put {
        /// Hash key of the contract.
        key: Vec<u8>,
        /// Value to upsert in the contract.
        value: Vec<u8>,
        contract: Contract,
    },
    /// Fetch the current value from a contract corresponding to the provided key.
    Get {
        /// Hash key of the contract.
        key: Vec<u8>,
        contract: bool,
    },
}

#[cfg(test)]
pub(crate) mod test_utils {
    use arbitrary::{Arbitrary, Unstructured};
    use rand::Rng;

    use super::*;

    pub(crate) struct MemoryEventsGen {
        rnd_bytes: [u8; 128],
    }

    impl MemoryEventsGen {
        pub fn new() -> Self {
            let mut rng = rand::thread_rng();
            let mut rnd_bytes = [0u8; 128];
            rng.fill(&mut rnd_bytes);
            MemoryEventsGen { rnd_bytes }
        }

        fn gen_new_event(&self) -> UserEvent {
            let mut unst = Unstructured::new(&self.rnd_bytes);
            UserEvent::arbitrary(&mut unst).expect("failed generating a random user event")
        }
    }

    #[async_trait::async_trait]
    impl UserEventsProxy for MemoryEventsGen {
        /// # Cancellation Safety
        /// This future must be safe to cancel.
        async fn recv(&self) -> UserEvent {
            self.gen_new_event()
        }
    }
}
