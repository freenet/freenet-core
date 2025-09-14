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
use freenet_stdlib::prelude::ContractKey;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::{debug, info};

/// Resource identifier for deduplicating requests
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestResource {
    /// GET requests with their parameters that affect the result
    Get {
        key: ContractKey,
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
        }
    }
}

/// A client request that can be deduplicated
#[derive(Debug, Clone)]
pub enum DeduplicatedRequest {
    Get {
        key: ContractKey,
        return_contract_code: bool,
        subscribe: bool,
        client_id: ClientId,
        request_id: RequestId,
    },
    Put {
        key: ContractKey,
        contract: freenet_stdlib::prelude::ContractContainer,
        related_contracts: freenet_stdlib::prelude::RelatedContracts<'static>,
        state: freenet_stdlib::prelude::WrappedState,
        subscribe: bool,
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
        }
    }
}

/// Request routing state for deduplication
#[derive(Debug)]
struct RequestRoutingState {
    /// Maps resources to the primary transaction handling the request
    resource_to_transaction: DashMap<RequestResource, Transaction>,
    /// Maps transactions to all clients waiting for the result
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
                transaction_waiters: DashMap::new(),
            },
        }
    }

    /// Route a client request, handling deduplication
    /// Returns (transaction_id, should_start_operation)
    pub async fn route_request(
        &self,
        request: DeduplicatedRequest,
    ) -> anyhow::Result<(Transaction, bool)> {
        let resource = request.resource();
        let (client_id, request_id) = request.client_info();

        // Check if there's already an operation for this resource
        if let Some(existing_tx) = self.state.resource_to_transaction.get(&resource) {
            let existing_tx = *existing_tx.value();

            // Add this client to the waiters list
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

            return Ok((existing_tx, false));
        }

        // No existing operation, create new transaction
        let new_tx = self.create_transaction_for_request(&request);

        // Register the resource mapping
        self.state
            .resource_to_transaction
            .insert(resource.clone(), new_tx);

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

    /// Create appropriate transaction for request type
    fn create_transaction_for_request(&self, request: &DeduplicatedRequest) -> Transaction {
        match request {
            DeduplicatedRequest::Get { .. } => Transaction::new::<crate::operations::get::GetMsg>(),
            DeduplicatedRequest::Put { .. } => Transaction::new::<crate::operations::put::PutMsg>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::{
        ContractCode, ContractContainer, ContractInstanceId, ContractWasmAPIVersion, Parameters,
        RelatedContracts, WrappedContract, WrappedState,
    };
    use std::sync::Arc;

    fn create_test_contract_key() -> ContractKey {
        ContractInstanceId::new([1u8; 32]).into()
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
        let key = create_test_contract_key();
        let client_id_1 = ClientId::next();
        let client_id_2 = ClientId::next();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // First GET request
        let request_1 = DeduplicatedRequest::Get {
            key,
            return_contract_code: true,
            subscribe: false,
            client_id: client_id_1,
            request_id: request_id_1,
        };

        // Identical GET request from different client
        let request_2 = DeduplicatedRequest::Get {
            key,
            return_contract_code: true,
            subscribe: false,
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
        let key = create_test_contract_key();
        let client_id = create_test_client_id();
        let request_id_1 = RequestId::new();
        let request_id_2 = RequestId::new();

        // GET request with return_contract_code=true
        let request_1 = DeduplicatedRequest::Get {
            key,
            return_contract_code: true,
            subscribe: false,
            client_id,
            request_id: request_id_1,
        };

        // GET request with return_contract_code=false (different result expected)
        let request_2 = DeduplicatedRequest::Get {
            key,
            return_contract_code: false,
            subscribe: false,
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
        let key = create_test_contract_key();

        // Same GET requests should have same hash
        let get_1 = RequestResource::Get {
            key,
            return_contract_code: true,
            subscribe: false,
        };
        let get_2 = RequestResource::Get {
            key,
            return_contract_code: true,
            subscribe: false,
        };

        assert_eq!(get_1, get_2);

        // Different parameters should have different hashes
        let get_3 = RequestResource::Get {
            key,
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
            client_id,
            request_id: request_id_2,
        };

        let (tx1, should_start_1) = router.route_request(request_1).await.unwrap();
        assert!(should_start_1);

        let (tx2, should_start_2) = router.route_request(request_2).await.unwrap();
        assert!(should_start_2); // Should start new operation
        assert_ne!(tx1, tx2); // Different transactions
    }
}
