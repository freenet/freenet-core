use crate::{
    contract::{Contract, ContractKey},
    operations::put::ContractValue,
};

#[async_trait::async_trait]
pub(crate) trait UserEventsProxy {
    async fn recv(&mut self) -> UserEvent;
}

// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
#[derive(arbitrary::Arbitrary)]
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
}

pub(crate) mod test_utils {
    use arbitrary::{Arbitrary, Unstructured};

    use super::*;
    use crate::test_utils::random_bytes_128;

    pub(crate) struct MemoryEventsGen;

    impl MemoryEventsGen {
        pub fn new() -> Self {
            Self
        }

        fn gen_new_event(&mut self) -> UserEvent {
            let bytes = random_bytes_128();
            let mut unst = Unstructured::new(&bytes);
            UserEvent::arbitrary(&mut unst).expect("failed gen arb data")
        }
    }

    #[async_trait::async_trait]
    impl UserEventsProxy for MemoryEventsGen {
        /// # Cancellation Safety
        /// This future must be safe to cancel.
        async fn recv(&mut self) -> UserEvent {
            self.gen_new_event()
        }
    }
}
