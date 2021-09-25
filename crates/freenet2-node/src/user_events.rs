#[async_trait::async_trait]
pub(crate) trait UserEventsProxy {
    async fn recv(&self) -> UserEvent;
}

pub(crate) enum UserEvent {
    /// Update or insert a new value in a contract corresponding with the provided key.
    Put {
        /// Hash key of the contract.
        key: Vec<u8>,
        /// Value to upsert in the contract.
        value: Vec<u8>,
    },
    /// Fetch the current value from a contrart corresponding to the provided key.
    Get {
        /// Hash key of the contract.
        key: Vec<u8>,
    },
}

pub(crate) mod test_utils {
    use super::*;

    pub(crate) struct MemoryEventsGen {}

    #[async_trait::async_trait]
    impl UserEventsProxy for MemoryEventsGen {
        /// # Cancellation Safety
        /// This future must be safe to cancel.
        async fn recv(&self) -> UserEvent {
            todo!()
        }
    }
}
