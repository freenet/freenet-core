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

    use super::*;
    use crate::test::random_bytes_128;

    pub(crate) struct MemoryEventsGen;

    impl MemoryEventsGen {
        pub fn new() -> Self {
            Self
        }

        fn gen_new_event(&self) -> UserEvent {
            let bytes = random_bytes_128();
            let mut unst = Unstructured::new(&bytes);
            UserEvent::arbitrary(&mut unst).expect("failed gen arb data")
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
