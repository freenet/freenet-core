//! Request Router for operation deduplication and routing
//!
//! This module provides a centralized request routing service that handles
//! operation deduplication before network operations are created. It sits
//! between client requests and the operation layer, ensuring that multiple
//! client requests for the same resource are efficiently coalesced.

use crate::{
    client_events::{ClientId, RequestId},
    message::Transaction,
};
use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::{debug, info};

/// Resource identifier for deduplicating requests
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestResource {
    /// GET requests with their parameters that affect the result
    /// Uses instance_id since clients may not know the full key
    Get {
        key: ContractInstanceId,
        return_contract_code: bool,
        subscribe: bool,
    },
    /// PUT requests with their parameters that affect the operation
    Put {
        key: ContractKey,
        contract: freenet_stdlib::prelude::ContractContainer,
        related_contracts: freenet_stdlib::prelude::RelatedContracts<'static>,
        state: freenet_stdlib::prelude::WrappedState,
        subscribe: bool,
    },
    /// SUBSCRIBE requests - multiple clients subscribing to same contract should be deduplicated
    /// Uses instance_id since clients may not know the full key
    Subscribe { key: ContractInstanceId },
    /// UPDATE requests with their parameters that affect the operation
    Update {
        key: ContractKey,
        update_data: freenet_stdlib::prelude::UpdateData<'static>,
        related_contracts: freenet_stdlib::prelude::RelatedContracts<'static>,
    },
}

impl Hash for RequestResource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            RequestResource::Get {
                key,
                return_contract_code,
                subscribe,
            } => {
                // Hash discriminant for GET variant
                0u8.hash(state);
                key.hash(state);
                return_contract_code.hash(state);
                subscribe.hash(state);
            }
            RequestResource::Put {
                key,
                contract,
                related_contracts,
                state: wrapped_state,
                subscribe,
            } => {
                // Hash discriminant for PUT variant
                1u8.hash(state);
                key.hash(state);
                // Hash contract by its key (sufficient for deduplication since key is unique)
                contract.key().hash(state);
                // For complex types, we'll serialize to bytes and hash that
                // This ensures different contracts/states produce different hashes
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                for (key, _) in related_contracts.states() {
                    key.hash(&mut hasher);
                }
                wrapped_state.hash(&mut hasher);
                hasher.finish().hash(state);
                subscribe.hash(state);
            }
            RequestResource::Subscribe { key } => {
                // Hash discriminant for SUBSCRIBE variant
                2u8.hash(state);
                key.hash(state);
            }
            RequestResource::Update {
                key,
                update_data,
                related_contracts,
            } => {
                // Hash discriminant for UPDATE variant
                3u8.hash(state);
                key.hash(state);
                // For complex types, we'll serialize to bytes and hash that
                // This ensures different update data/related_contracts produce different hashes
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                for (key, _) in related_contracts.states() {
                    key.hash(&mut hasher);
                }
                // Hash the update data - works for all variants
                match update_data {
                    freenet_stdlib::prelude::UpdateData::State(s) => {
                        0u8.hash(&mut hasher);
                        s.hash(&mut hasher);
                    }
                    freenet_stdlib::prelude::UpdateData::Delta(d) => {
                        1u8.hash(&mut hasher);
                        d.hash(&mut hasher);
                    }
                    freenet_stdlib::prelude::UpdateData::StateAndDelta { state, delta } => {
                        2u8.hash(&mut hasher);
                        state.hash(&mut hasher);
                        delta.hash(&mut hasher);
                    }
                    freenet_stdlib::prelude::UpdateData::RelatedState { related_to, state } => {
                        3u8.hash(&mut hasher);
                        related_to.hash(&mut hasher);
                        state.hash(&mut hasher);
                    }
                    freenet_stdlib::prelude::UpdateData::RelatedDelta { related_to, delta } => {
                        4u8.hash(&mut hasher);
                        related_to.hash(&mut hasher);
                        delta.hash(&mut hasher);
                    }
                    freenet_stdlib::prelude::UpdateData::RelatedStateAndDelta {
                        related_to,
                        state,
                        delta,
                    } => {
                        5u8.hash(&mut hasher);
                        related_to.hash(&mut hasher);
                        state.hash(&mut hasher);
                        delta.hash(&mut hasher);
                    }
                }
                hasher.finish().hash(state);
            }
        }
    }
}

/// A client request that can be deduplicated
#[derive(Debug, Clone)]
pub enum DeduplicatedRequest {
    Get {
        /// Client requests use instance_id since they may not know the full key
        key: ContractInstanceId,
        return_contract_code: bool,
        subscribe: bool,
        /// Not used for deduplication, but carried through for future use.
        #[allow(dead_code)]
        blocking_subscribe: bool,
        client_id: ClientId,
        request_id: RequestId,
    },
    Put {
        key: ContractKey,
        contract: freenet_stdlib::prelude::ContractContainer,
        related_contracts: freenet_stdlib::prelude::RelatedContracts<'static>,
        state: freenet_stdlib::prelude::WrappedState,
        subscribe: bool,
        /// Not used for deduplication, but carried through for future use.
        #[allow(dead_code)]
        blocking_subscribe: bool,
        client_id: ClientId,
        request_id: RequestId,
    },
    /// Note: Currently unused - Subscribe operations bypass deduplication to avoid
    /// race conditions with instant-completion. Kept for potential future use.
    #[allow(dead_code)]
    Subscribe {
        /// Uses instance_id since clients may not know the full key
        key: ContractInstanceId,
        client_id: ClientId,
        request_id: RequestId,
    },
    Update {
        key: ContractKey,
        update_data: freenet_stdlib::prelude::UpdateData<'static>,
        related_contracts: freenet_stdlib::prelude::RelatedContracts<'static>,
        client_id: ClientId,
        request_id: RequestId,
    },
}

impl DeduplicatedRequest {
    pub fn resource(&self) -> RequestResource {
        match self {
            DeduplicatedRequest::Get {
                key,
                return_contract_code,
                subscribe,
                ..
            } => RequestResource::Get {
                key: *key,
                return_contract_code: *return_contract_code,
                subscribe: *subscribe,
            },
            DeduplicatedRequest::Put {
                key,
                contract,
                related_contracts,
                state,
                subscribe,
                ..
            } => RequestResource::Put {
                key: *key,
                contract: contract.clone(),
                related_contracts: related_contracts.clone(),
                state: state.clone(),
                subscribe: *subscribe,
            },
            DeduplicatedRequest::Subscribe { key, .. } => RequestResource::Subscribe { key: *key },
            DeduplicatedRequest::Update {
                key,
                update_data,
                related_contracts,
                ..
            } => RequestResource::Update {
                key: *key,
                update_data: update_data.clone(),
                related_contracts: related_contracts.clone(),
            },
        }
    }

    pub fn client_info(&self) -> (ClientId, RequestId) {
        match self {
            DeduplicatedRequest::Get {
                client_id,
                request_id,
                ..
            } => (*client_id, *request_id),
            DeduplicatedRequest::Put {
                client_id,
                request_id,
                ..
            } => (*client_id, *request_id),
            DeduplicatedRequest::Subscribe {
                client_id,
                request_id,
                ..
            } => (*client_id, *request_id),
            DeduplicatedRequest::Update {
                client_id,
                request_id,
                ..
            } => (*client_id, *request_id),
        }
    }
}

/// Request routing state for deduplication
#[derive(Debug)]
struct RequestRoutingState {
    /// Maps resources to the primary transaction handling the request
    resource_to_transaction: DashMap<RequestResource, Transaction>,
    /// Reverse mapping for O(1) cleanup: transaction -> resource
    transaction_to_resource: DashMap<Transaction, RequestResource>,
    /// Maps transactions to all clients waiting for the result.
    ///
    /// Note: This map is used for tracking which clients are waiting for a transaction,
    /// but actual result delivery happens through a separate mechanism
    /// (`waiting_for_transaction_result` in ContractHandlerChannel). This map enables:
    /// 1. Detecting when a transaction has active waiters (for cleanup decisions)
    /// 2. Future extensibility for direct result fan-out if needed
    /// 3. Debugging/monitoring of client request patterns
    transaction_waiters: DashMap<Transaction, Vec<(ClientId, RequestId)>>,
}

/// Request Router handles deduplication of client requests before operation creation
pub struct RequestRouter {
    state: RequestRoutingState,
}

impl RequestRouter {
    pub fn new() -> Self {
        Self {
            state: RequestRoutingState {
                resource_to_transaction: DashMap::new(),
                transaction_to_resource: DashMap::new(),
                transaction_waiters: DashMap::new(),
            },
        }
    }

    /// Route a client request, handling deduplication
    /// Returns (transaction_id, should_start_operation)
    ///
    /// This method is atomic with respect to concurrent calls - it uses DashMap's
    /// entry API to prevent TOCTOU races where two threads could both see no
    /// existing entry and create duplicate operations.
    pub async fn route_request(
        &self,
        request: DeduplicatedRequest,
    ) -> anyhow::Result<(Transaction, bool)> {
        let resource = request.resource();
        let (client_id, request_id) = request.client_info();

        // Use entry API for atomic check-and-insert to prevent TOCTOU races.
        // This ensures that if two threads race to route the same resource,
        // only one will create a new operation.
        use dashmap::mapref::entry::Entry;

        match self.state.resource_to_transaction.entry(resource.clone()) {
            Entry::Occupied(mut entry) => {
                // Existing operation found - deduplicate by adding client to waiters,
                // but first verify that the transaction is still active.
                let existing_tx = *entry.get();

                // If the reverse mapping is missing, this transaction has been completed
                // concurrently. Treat this as if there was no existing operation and
                // create a new one instead of adding waiters to a completed transaction.
                if !self
                    .state
                    .transaction_to_resource
                    .contains_key(&existing_tx)
                {
                    // The transaction was already completed; start a fresh operation.
                    let new_tx = self.create_transaction_for_request(&request);

                    // Overwrite the stale resource -> transaction mapping with the new one.
                    entry.insert(new_tx);

                    // Insert reverse mapping for O(1) cleanup.
                    self.state
                        .transaction_to_resource
                        .insert(new_tx, resource.clone());

                    // Initialize waiters list with this client.
                    self.state
                        .transaction_waiters
                        .insert(new_tx, vec![(client_id, request_id)]);

                    info!(
                        transaction = %new_tx,
                        resource = ?resource,
                        client = %client_id,
                        request = %request_id,
                        "Created new operation after detecting completed transaction"
                    );

                    return Ok((new_tx, true));
                }

                // Transaction is still active; add this client to the waiters list.
                // Use entry API here too to handle the race where complete_operation
                // might have removed this transaction between our check and insert.
                self.state
                    .transaction_waiters
                    .entry(existing_tx)
                    .or_default()
                    .push((client_id, request_id));

                debug!(
                    transaction = %existing_tx,
                    resource = ?resource,
                    client = %client_id,
                    request = %request_id,
                    "Reusing existing operation - client added to waiters"
                );

                Ok((existing_tx, false))
            }
            Entry::Vacant(entry) => {
                // No existing operation - create new transaction atomically
                let new_tx = self.create_transaction_for_request(&request);

                // Insert the resource -> transaction mapping
                entry.insert(new_tx);

                // Insert reverse mapping for O(1) cleanup
                self.state
                    .transaction_to_resource
                    .insert(new_tx, resource.clone());

                // Initialize waiters list with this client
                self.state
                    .transaction_waiters
                    .insert(new_tx, vec![(client_id, request_id)]);

                info!(
                    transaction = %new_tx,
                    resource = ?resource,
                    client = %client_id,
                    request = %request_id,
                    "Created new operation - starting network request"
                );

                Ok((new_tx, true))
            }
        }
    }

    /// Create appropriate transaction for request type
    fn create_transaction_for_request(&self, request: &DeduplicatedRequest) -> Transaction {
        match request {
            DeduplicatedRequest::Get { .. } => Transaction::new::<crate::operations::get::GetMsg>(),
            DeduplicatedRequest::Put { .. } => Transaction::new::<crate::operations::put::PutMsg>(),
            DeduplicatedRequest::Subscribe { .. } => {
                Transaction::new::<crate::operations::subscribe::SubscribeMsg>()
            }
            DeduplicatedRequest::Update { .. } => {
                Transaction::new::<crate::operations::update::UpdateMsg>()
            }
        }
    }

    /// Mark an operation as complete, cleaning up routing state.
    ///
    /// This MUST be called when a transaction completes (success or failure) to prevent
    /// stale entries in `resource_to_transaction` from blocking subsequent requests for
    /// the same resource.
    ///
    /// Without this cleanup, later requests for the same resource would incorrectly
    /// deduplicate against the completed transaction, causing clients to wait forever
    /// for a result that will never arrive.
    ///
    /// This method is idempotent - calling it multiple times for the same transaction
    /// is safe and has no effect after the first call.
    pub fn complete_operation(&self, tx: Transaction) {
        // Use reverse mapping for O(1) lookup instead of O(n) iteration
        if let Some((_, resource)) = self.state.transaction_to_resource.remove(&tx) {
            // Remove the resource -> transaction mapping
            self.state.resource_to_transaction.remove(&resource);

            debug!(
                transaction = %tx,
                resource = ?resource,
                "Operation completed - cleaned up routing state"
            );
        }

        // Always try to remove waiters (may have been added by a racing route_request)
        self.state.transaction_waiters.remove(&tx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GlobalExecutor;
    use freenet_stdlib::prelude::{
        CodeHash, ContractCode, ContractContainer, ContractInstanceId, ContractWasmAPIVersion,
        Parameters, RelatedContracts, WrappedContract, WrappedState,
    };
    use std::sync::Arc;

    fn create_test_instance_id() -> ContractInstanceId {
        ContractInstanceId::new([1u8; 32])
    }

    fn create_test_contract_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    fn create_test_contract() -> ContractContainer {
        const PARAMS: &[u8] = &[5, 6];
        ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![1, 2, 3, 4])),
            Parameters::from(PARAMS),
        )))
    }

    fn create_test_client_id() -> ClientId {
        ClientId::next()
    }

    fn create_test_related_contracts() -> RelatedContracts<'static> {
        RelatedContracts::default()
    }

    fn create_test_wrapped_state() -> WrappedState {
        WrappedState::new(vec![7, 8, 9, 10])
    }

    #[tokio::test]
    async fn test_request_router_creation() {
        let router = RequestRouter::new();
        // Just verify it can be created without parameters
        assert!(std::ptr::addr_of!(router).is_aligned());
    }

    #[tokio::test]
    async fn test_get_request_deduplication() {
        let router = RequestRouter::new();
        let instance_id = create_test_instance_id();
        let client_id_1 = ClientId::next();
        let client_id_2 = ClientId::next();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // First GET request
        let request_1 = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
            client_id: client_id_1,
            request_id: request_id_1,
        };

        // Identical GET request from different client
        let request_2 = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
            client_id: client_id_2,
            request_id: request_id_2,
        };

        // First request should create new operation
        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        // Second identical request should reuse existing operation
        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(!should_start_2);
        assert_eq!(tx1, tx2);
    }

    #[tokio::test]
    async fn test_get_request_different_parameters_no_deduplication() {
        let router = RequestRouter::new();
        let instance_id = create_test_instance_id();
        let client_id = create_test_client_id();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // GET request with return_contract_code=true
        let request_1 = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
            client_id,
            request_id: request_id_1,
        };

        // GET request with return_contract_code=false (different result expected)
        let request_2 = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: false,
            subscribe: false,
            blocking_subscribe: false,
            client_id,
            request_id: request_id_2,
        };

        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(should_start_2); // Should start new operation
        assert_ne!(tx1, tx2); // Different transactions
    }

    #[test]
    fn test_request_resource_hash_consistency() {
        let instance_id = create_test_instance_id();

        // Same GET requests should have same hash
        let get_1 = RequestResource::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
        };
        let get_2 = RequestResource::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
        };

        assert_eq!(get_1, get_2);

        // Different parameters should have different hashes
        let get_3 = RequestResource::Get {
            key: instance_id,
            return_contract_code: false, // Different parameter
            subscribe: false,
        };

        assert_ne!(get_1, get_3);
    }

    #[tokio::test]
    async fn test_put_request_deduplication() {
        let router = RequestRouter::new();
        let key = create_test_contract_key();
        let contract = create_test_contract();
        let related_contracts = create_test_related_contracts();
        let state = create_test_wrapped_state();
        let client_id_1 = ClientId::next();
        let client_id_2 = ClientId::next();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // First PUT request
        let request_1 = DeduplicatedRequest::Put {
            key,
            contract: contract.clone(),
            related_contracts: related_contracts.clone(),
            state: state.clone(),
            subscribe: false,
            blocking_subscribe: false,
            client_id: client_id_1,
            request_id: request_id_1,
        };

        // Identical PUT request from different client
        let request_2 = DeduplicatedRequest::Put {
            key,
            contract: contract.clone(),
            related_contracts: related_contracts.clone(),
            state: state.clone(),
            subscribe: false,
            blocking_subscribe: false,
            client_id: client_id_2,
            request_id: request_id_2,
        };

        // First request should create new operation
        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        // Second identical request should reuse existing operation
        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(!should_start_2);
        assert_eq!(tx1, tx2);
    }

    #[tokio::test]
    async fn test_put_request_different_state_no_deduplication() {
        let router = RequestRouter::new();
        let key = create_test_contract_key();
        let contract = create_test_contract();
        let related_contracts = create_test_related_contracts();
        let state1 = create_test_wrapped_state();
        let state2 = WrappedState::new(vec![11, 12, 13, 14]); // Different state
        let client_id = create_test_client_id();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // PUT request with first state
        let request_1 = DeduplicatedRequest::Put {
            key,
            contract: contract.clone(),
            related_contracts: related_contracts.clone(),
            state: state1,
            subscribe: false,
            blocking_subscribe: false,
            client_id,
            request_id: request_id_1,
        };

        // PUT request with different state (should not be deduplicated)
        let request_2 = DeduplicatedRequest::Put {
            key,
            contract: contract.clone(),
            related_contracts: related_contracts.clone(),
            state: state2,
            subscribe: false,
            blocking_subscribe: false,
            client_id,
            request_id: request_id_2,
        };

        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(should_start_2); // Should start new operation
        assert_ne!(tx1, tx2); // Different transactions
    }

    #[tokio::test]
    async fn test_put_request_different_subscribe_parameter_no_deduplication() {
        let router = RequestRouter::new();
        let key = create_test_contract_key();
        let contract = create_test_contract();
        let related_contracts = create_test_related_contracts();
        let state = create_test_wrapped_state();
        let client_id = create_test_client_id();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // PUT request with subscribe=false
        let request_1 = DeduplicatedRequest::Put {
            key,
            contract: contract.clone(),
            related_contracts: related_contracts.clone(),
            state: state.clone(),
            subscribe: false,
            blocking_subscribe: false,
            client_id,
            request_id: request_id_1,
        };

        // PUT request with subscribe=true (different result expected)
        let request_2 = DeduplicatedRequest::Put {
            key,
            contract: contract.clone(),
            related_contracts: related_contracts.clone(),
            state: state.clone(),
            subscribe: true,
            blocking_subscribe: false,
            client_id,
            request_id: request_id_2,
        };

        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(should_start_2); // Should start new operation
        assert_ne!(tx1, tx2); // Different transactions
    }

    #[tokio::test]
    async fn test_subscribe_request_deduplication() {
        let router = RequestRouter::new();
        let instance_id = create_test_instance_id();
        let client_id_1 = ClientId::next();
        let client_id_2 = ClientId::next();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // First SUBSCRIBE request
        let request_1 = DeduplicatedRequest::Subscribe {
            key: instance_id,
            client_id: client_id_1,
            request_id: request_id_1,
        };

        // Identical SUBSCRIBE request from different client (should be deduplicated)
        let request_2 = DeduplicatedRequest::Subscribe {
            key: instance_id,
            client_id: client_id_2,
            request_id: request_id_2,
        };

        // First request should create new operation
        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        // Second identical request should reuse existing operation
        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(!should_start_2);
        assert_eq!(tx1, tx2);
    }

    #[tokio::test]
    async fn test_subscribe_different_contract_no_deduplication() {
        let router = RequestRouter::new();
        let instance_id_1 = create_test_instance_id();
        let instance_id_2 = ContractInstanceId::new([2u8; 32]); // Different contract
        let client_id = create_test_client_id();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // SUBSCRIBE request for first contract
        let request_1 = DeduplicatedRequest::Subscribe {
            key: instance_id_1,
            client_id,
            request_id: request_id_1,
        };

        // SUBSCRIBE request for different contract (should not be deduplicated)
        let request_2 = DeduplicatedRequest::Subscribe {
            key: instance_id_2,
            client_id,
            request_id: request_id_2,
        };

        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(should_start_2); // Should start new operation
        assert_ne!(tx1, tx2); // Different transactions
    }

    #[tokio::test]
    async fn test_update_request_deduplication() {
        let router = RequestRouter::new();
        let key = create_test_contract_key();
        let related_contracts = create_test_related_contracts();
        let new_state = create_test_wrapped_state();
        let update_data = freenet_stdlib::prelude::UpdateData::State(
            freenet_stdlib::prelude::State::from(new_state),
        );
        let client_id_1 = ClientId::next();
        let client_id_2 = ClientId::next();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // First UPDATE request
        let request_1 = DeduplicatedRequest::Update {
            key,
            update_data: update_data.clone(),
            related_contracts: related_contracts.clone(),
            client_id: client_id_1,
            request_id: request_id_1,
        };

        // Identical UPDATE request from different client (should be deduplicated)
        let request_2 = DeduplicatedRequest::Update {
            key,
            update_data: update_data.clone(),
            related_contracts: related_contracts.clone(),
            client_id: client_id_2,
            request_id: request_id_2,
        };

        // First request should create new operation
        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        // Second identical request should reuse existing operation
        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(!should_start_2);
        assert_eq!(tx1, tx2);
    }

    #[tokio::test]
    async fn test_update_request_different_state_no_deduplication() {
        let router = RequestRouter::new();
        let key = create_test_contract_key();
        let related_contracts = create_test_related_contracts();
        let state1 = create_test_wrapped_state();
        let state2 = WrappedState::new(vec![15, 16, 17, 18]); // Different state
        let update_data1 = freenet_stdlib::prelude::UpdateData::State(
            freenet_stdlib::prelude::State::from(state1),
        );
        let update_data2 = freenet_stdlib::prelude::UpdateData::State(
            freenet_stdlib::prelude::State::from(state2),
        );
        let client_id = create_test_client_id();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // UPDATE request with first state
        let request_1 = DeduplicatedRequest::Update {
            key,
            update_data: update_data1,
            related_contracts: related_contracts.clone(),
            client_id,
            request_id: request_id_1,
        };

        // UPDATE request with different state (should not be deduplicated)
        let request_2 = DeduplicatedRequest::Update {
            key,
            update_data: update_data2,
            related_contracts: related_contracts.clone(),
            client_id,
            request_id: request_id_2,
        };

        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(should_start_2); // Should start new operation
        assert_ne!(tx1, tx2); // Different transactions
    }

    #[tokio::test]
    async fn test_update_request_different_contract_no_deduplication() {
        let router = RequestRouter::new();
        let key1 = create_test_contract_key();
        let key2 = ContractKey::from_id_and_code(
            ContractInstanceId::new([3u8; 32]),
            CodeHash::new([4u8; 32]),
        ); // Different contract
        let related_contracts = create_test_related_contracts();
        let new_state = create_test_wrapped_state();
        let update_data = freenet_stdlib::prelude::UpdateData::State(
            freenet_stdlib::prelude::State::from(new_state),
        );
        let client_id = create_test_client_id();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // UPDATE request for first contract
        let request_1 = DeduplicatedRequest::Update {
            key: key1,
            update_data: update_data.clone(),
            related_contracts: related_contracts.clone(),
            client_id,
            request_id: request_id_1,
        };

        // UPDATE request for different contract (should not be deduplicated)
        let request_2 = DeduplicatedRequest::Update {
            key: key2,
            update_data: update_data.clone(),
            related_contracts: related_contracts.clone(),
            client_id,
            request_id: request_id_2,
        };

        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(should_start_2); // Should start new operation
        assert_ne!(tx1, tx2); // Different transactions
    }

    #[test]
    fn test_all_operation_types_resource_hash_consistency() {
        let instance_id = create_test_instance_id();
        let key = create_test_contract_key();
        let related_contracts = create_test_related_contracts();
        let state = create_test_wrapped_state();

        // Same operations should have same hashes
        let get_1 = RequestResource::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
        };
        let get_2 = RequestResource::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
        };
        assert_eq!(get_1, get_2);

        let subscribe_1 = RequestResource::Subscribe { key: instance_id };
        let subscribe_2 = RequestResource::Subscribe { key: instance_id };
        assert_eq!(subscribe_1, subscribe_2);

        let update_data = freenet_stdlib::prelude::UpdateData::State(
            freenet_stdlib::prelude::State::from(state.clone()),
        );
        let update_1 = RequestResource::Update {
            key,
            update_data: update_data.clone(),
            related_contracts: related_contracts.clone(),
        };
        let update_2 = RequestResource::Update {
            key,
            update_data: update_data.clone(),
            related_contracts: related_contracts.clone(),
        };
        assert_eq!(update_1, update_2);

        // Different operation types should have different hashes
        assert_ne!(get_1, subscribe_1);
        assert_ne!(get_1, update_1);
        assert_ne!(subscribe_1, update_1);

        // Same operation type with different parameters should have different hashes
        let subscribe_different = RequestResource::Subscribe {
            key: ContractInstanceId::new([4u8; 32]),
        };
        assert_ne!(subscribe_1, subscribe_different);

        let update_data_different = freenet_stdlib::prelude::UpdateData::State(
            freenet_stdlib::prelude::State::from(WrappedState::new(vec![99, 100])),
        );
        let update_different = RequestResource::Update {
            key,
            update_data: update_data_different,
            related_contracts: related_contracts.clone(),
        };
        assert_ne!(update_1, update_different);
    }

    /// Regression test: After an operation completes, a new request for the same
    /// resource should start a fresh operation, not reuse the stale transaction.
    ///
    /// This bug caused the River UI to hang indefinitely on technic (2025-12-27):
    /// 1. First GET for River contract completed successfully at 16:35
    /// 2. User refreshed the page at 17:00
    /// 3. Router found stale entry in resource_to_transaction, returned old tx
    /// 4. Client was registered to wait for already-completed transaction
    /// 5. Browser hung forever waiting for a result that would never arrive
    #[tokio::test]
    async fn test_request_after_completion_starts_new_operation() {
        let router = RequestRouter::new();
        let instance_id = create_test_instance_id();
        let client_id_1 = ClientId::next();
        let client_id_2 = ClientId::next();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // First GET request - should create new operation
        let request_1 = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
            client_id: client_id_1,
            request_id: request_id_1,
        };

        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1, "First request should start new operation");

        // Simulate operation completing (result delivered to clients)
        router.complete_operation(tx1);

        // Second GET request for SAME resource after completion
        // This MUST start a new operation, not reuse the stale transaction
        let request_2 = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
            client_id: client_id_2,
            request_id: request_id_2,
        };

        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(
            should_start_2,
            "Request after completion MUST start new operation, not reuse stale tx"
        );
        assert_ne!(
            tx1, tx2,
            "New operation should have different transaction ID"
        );
    }

    /// Test that complete_operation properly cleans up all maps
    #[tokio::test]
    async fn test_complete_operation_cleans_up_state() {
        let router = RequestRouter::new();
        let instance_id = create_test_instance_id();
        let client_id = ClientId::next();
        let request_id = RequestId::new();

        let request = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
            client_id,
            request_id,
        };

        let (tx, _) = router.route_request(request).await.unwrap();

        // Verify state exists before cleanup
        assert!(
            !router.state.resource_to_transaction.is_empty(),
            "resource_to_transaction should have entry"
        );
        assert!(
            !router.state.transaction_to_resource.is_empty(),
            "transaction_to_resource should have entry"
        );
        assert!(
            !router.state.transaction_waiters.is_empty(),
            "transaction_waiters should have entry"
        );

        // Complete the operation
        router.complete_operation(tx);

        // Verify all state is cleaned up
        assert!(
            router.state.resource_to_transaction.is_empty(),
            "resource_to_transaction should be empty after completion"
        );
        assert!(
            router.state.transaction_to_resource.is_empty(),
            "transaction_to_resource should be empty after completion"
        );
        assert!(
            router.state.transaction_waiters.is_empty(),
            "transaction_waiters should be empty after completion"
        );
    }

    /// Test that complete_operation is idempotent (safe to call multiple times)
    #[tokio::test]
    async fn test_complete_operation_is_idempotent() {
        let router = RequestRouter::new();
        let instance_id = create_test_instance_id();
        let client_id = ClientId::next();
        let request_id = RequestId::new();

        let request = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
            client_id,
            request_id,
        };

        let (tx, _) = router.route_request(request).await.unwrap();

        // Complete the operation multiple times - should not panic
        router.complete_operation(tx);
        router.complete_operation(tx);
        router.complete_operation(tx);

        // All maps should be empty
        assert!(router.state.resource_to_transaction.is_empty());
        assert!(router.state.transaction_to_resource.is_empty());
        assert!(router.state.transaction_waiters.is_empty());
    }

    /// Test concurrent routing uses atomic entry API correctly
    #[tokio::test]
    async fn test_concurrent_routing_deduplication() {
        use std::sync::Arc;
        use tokio::sync::Barrier;

        let router = Arc::new(RequestRouter::new());
        let instance_id = create_test_instance_id();
        let barrier = Arc::new(Barrier::new(10));

        // Spawn 10 concurrent requests for the same resource
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let router = router.clone();
                let barrier = barrier.clone();
                GlobalExecutor::spawn(async move {
                    let client_id = ClientId::next();
                    let request_id = RequestId::new();

                    let request = DeduplicatedRequest::Get {
                        key: instance_id,
                        return_contract_code: true,
                        subscribe: false,
                        blocking_subscribe: false,
                        client_id,
                        request_id,
                    };

                    // Wait for all tasks to be ready
                    barrier.wait().await;

                    // All route at the same time
                    let (tx, should_start) = router.route_request(request).await.unwrap();
                    (i, tx, should_start)
                })
            })
            .collect();

        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Exactly one request should have started a new operation
        let starters: Vec<_> = results.iter().filter(|(_, _, started)| *started).collect();
        assert_eq!(
            starters.len(),
            1,
            "Exactly one request should start operation, got {}",
            starters.len()
        );

        // All requests should have the same transaction ID
        let first_tx = results[0].1;
        for (i, tx, _) in &results {
            assert_eq!(
                *tx, first_tx,
                "Request {} should have same tx as request 0",
                i
            );
        }

        // There should be exactly one entry in each map
        assert_eq!(router.state.resource_to_transaction.len(), 1);
        assert_eq!(router.state.transaction_to_resource.len(), 1);
        assert_eq!(router.state.transaction_waiters.len(), 1);

        // All 10 clients should be in the waiters list
        let waiters = router.state.transaction_waiters.get(&first_tx).unwrap();
        assert_eq!(waiters.len(), 10, "All 10 clients should be waiting");
    }

    /// Test the race condition between route_request and complete_operation.
    ///
    /// This tests the scenario where:
    /// 1. A request creates a transaction
    /// 2. The transaction completes and complete_operation is called
    /// 3. A new request arrives before resource_to_transaction is fully cleaned up
    /// 4. The new request should detect the stale entry and create a new operation
    #[tokio::test]
    async fn test_route_request_detects_completed_transaction() {
        let router = RequestRouter::new();
        let instance_id = create_test_instance_id();
        let client_id_1 = ClientId::next();
        let client_id_2 = ClientId::next();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // First request creates a new operation
        let request_1 = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
            client_id: client_id_1,
            request_id: request_id_1,
        };
        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        // Simulate partial completion: remove only the reverse mapping
        // This simulates the race where complete_operation started but hasn't
        // finished cleaning up resource_to_transaction yet
        router.state.transaction_to_resource.remove(&tx1);

        // Second request should detect that tx1 is no longer valid
        // and create a new operation instead of waiting for the completed one
        let request_2 = DeduplicatedRequest::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
            client_id: client_id_2,
            request_id: request_id_2,
        };
        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();

        // Should create a new operation since tx1 was detected as completed
        assert!(
            should_start_2,
            "Should start new operation for completed transaction"
        );
        assert_ne!(tx1, tx2, "Should get a different transaction ID");

        // The router should now have the new transaction registered
        assert!(router.state.transaction_to_resource.contains_key(&tx2));
        let resource = RequestResource::Get {
            key: instance_id,
            return_contract_code: true,
            subscribe: false,
        };
        assert_eq!(
            *router.state.resource_to_transaction.get(&resource).unwrap(),
            tx2
        );
    }
}
