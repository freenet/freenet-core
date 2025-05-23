#![allow(unused_imports, dead_code, clippy::all)]

#[cfg(feature = "trace")]
pub(crate) mod topology_generated;
use freenet_stdlib::prelude::ContractKey;
pub use topology_generated::*;

use crate::{message::Transaction, node::PeerId};

pub trait TryFromFbs<'a>: Sized + 'a {
    fn try_decode_fbs(buf: &'a [u8]) -> Result<Self, flatbuffers::InvalidFlatbuffer>;
}

pub enum PeerChange<'a> {
    AddedConnection(topology::AddedConnection<'a>),
    RemovedConnection(topology::RemovedConnection<'a>),
    Error(topology::Error<'a>),
}

pub enum ContractChange<'a> {
    PutRequest(topology::PutRequest<'a>),
    PutSuccess(topology::PutSuccess<'a>),
    PutFailure(topology::PutFailure<'a>),
    BroadcastEmitted(topology::BroadcastEmitted<'a>),
    BroadcastReceived(topology::BroadcastReceived<'a>),
    GetContract(topology::GetContract<'a>),
    SubscribedToContract(topology::SubscribedToContract<'a>),
    UpdateRequest(topology::UpdateRequest<'a>),
    UpdateSuccess(topology::UpdateSuccess<'a>),
    UpdateFailure(topology::UpdateFailure<'a>),
}

// TODO: Change this to EventWrapper
pub enum ChangesWrapper<'a> {
    ContractChange(ContractChange<'a>),
    PeerChange(PeerChange<'a>),
}

impl ContractChange<'_> {
    pub fn put_request_msg(
        transaction: String,
        contract: String,
        requester: String,
        target: String,
        timestamp: u64,
        contract_location: f64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let transaction = buf.create_string(transaction.as_str());
        let contract = buf.create_string(contract.as_str());
        let requester = buf.create_string(requester.as_str());
        let target = buf.create_string(target.as_str());
        let put_req = topology::PutRequest::create(
            &mut buf,
            &topology::PutRequestArgs {
                transaction: Some(transaction),
                key: Some(contract),
                requester: Some(requester),
                target: Some(target),
                timestamp,
                contract_location,
            },
        );
        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract),
                change_type: topology::ContractChangeType::PutRequest,
                change: Some(put_req.as_union_value()),
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn put_success_msg(
        transaction: String,
        contract: String,
        requester: String,
        target: String,
        timestamp: u64,
        contract_location: f64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let transaction = buf.create_string(transaction.as_str());
        let contract = buf.create_string(contract.as_str());
        let requester = buf.create_string(requester.as_str());
        let target = buf.create_string(target.as_str());
        let put_success = topology::PutSuccess::create(
            &mut buf,
            &topology::PutSuccessArgs {
                transaction: Some(transaction),
                key: Some(contract),
                requester: Some(requester),
                target: Some(target),
                timestamp,
                contract_location,
            },
        );
        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract),
                change_type: topology::ContractChangeType::PutSuccess,
                change: Some(put_success.as_union_value()),
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn put_failure_msg(
        transaction: impl AsRef<str>,
        contract: impl AsRef<str>,
        requester: impl AsRef<str>,
        target: impl AsRef<str>,
        timestamp: u64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let transaction = buf.create_string(transaction.as_ref());
        let contract = buf.create_string(contract.as_ref());
        let requester = buf.create_string(requester.as_ref());
        let target = buf.create_string(target.as_ref());
        let put_failure = topology::PutFailure::create(
            &mut buf,
            &topology::PutFailureArgs {
                transaction: Some(transaction),
                key: Some(contract),
                requester: Some(requester),
                target: Some(target),
                timestamp,
            },
        );
        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract),
                change_type: topology::ContractChangeType::PutFailure,
                change: Some(put_failure.as_union_value()),
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn broadcast_emitted_msg(
        transaction: impl AsRef<str>,
        upstream: impl AsRef<str>,
        broadcast_to: Vec<String>,
        broadcasted_to: usize,
        contract_key: impl AsRef<str>,
        sender: impl AsRef<str>,

        timestamp: u64,
        contract_location: f64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let transaction = buf.create_string(transaction.as_ref());
        let upstream = buf.create_string(upstream.as_ref());
        let broadcast_to = broadcast_to
            .iter()
            .map(|s| buf.create_string(s.as_str()))
            .collect::<Vec<_>>();
        let broadcast_to = buf.create_vector(&broadcast_to);
        let contract_key = buf.create_string(contract_key.as_ref());
        let sender = buf.create_string(sender.as_ref());
        let broadcast_emitted = topology::BroadcastEmitted::create(
            &mut buf,
            &topology::BroadcastEmittedArgs {
                transaction: Some(transaction),
                upstream: Some(upstream),
                broadcast_to: Some(broadcast_to),
                broadcasted_to: broadcasted_to as u32,
                key: Some(contract_key),
                sender: Some(sender),
                timestamp,
                contract_location,
            },
        );

        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract_key),
                change_type: topology::ContractChangeType::BroadcastEmitted,
                change: Some(broadcast_emitted.as_union_value()),
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn broadcast_received_msg(
        transaction: impl AsRef<str>,
        target: impl AsRef<str>,
        requester: impl AsRef<str>,
        contract_key: impl AsRef<str>,

        timestamp: u64,
        contract_location: f64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let transaction = buf.create_string(transaction.as_ref());
        let target = buf.create_string(target.as_ref());
        let requester = buf.create_string(requester.as_ref());
        let contract_key = buf.create_string(contract_key.as_ref());
        let broadcast_received = topology::BroadcastReceived::create(
            &mut buf,
            &topology::BroadcastReceivedArgs {
                transaction: Some(transaction),
                target: Some(target),
                requester: Some(requester),
                key: Some(contract_key),
                timestamp,
                contract_location,
            },
        );

        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract_key),
                change_type: topology::ContractChangeType::BroadcastReceived,
                change: Some(broadcast_received.as_union_value()),
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn get_contract_msg(
        requester: impl AsRef<str>,
        target: impl AsRef<str>,
        transaction: impl AsRef<str>,
        contract_key: impl AsRef<str>,
        contract_location: f64,
        timestamp: u64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let requester = buf.create_string(requester.as_ref());
        let target = buf.create_string(target.as_ref());
        let transaction = buf.create_string(transaction.as_ref());
        let contract_key = buf.create_string(contract_key.as_ref());
        let get_contract = topology::GetContract::create(
            &mut buf,
            &topology::GetContractArgs {
                requester: Some(requester),
                target: Some(target),
                transaction: Some(transaction),
                key: Some(contract_key),
                contract_location,
                timestamp,
            },
        );

        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract_key),
                change_type: topology::ContractChangeType::GetContract,
                change: Some(get_contract.as_union_value()),
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn subscribed_msg(
        requester: impl AsRef<str>,
        transaction: impl AsRef<str>,
        contract_key: impl AsRef<str>,
        contract_location: f64,
        at_peer: impl AsRef<str>,
        at_peer_location: f64,
        timestamp: u64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let requester = buf.create_string(requester.as_ref());
        let transaction = buf.create_string(transaction.as_ref());
        let contract_key = buf.create_string(contract_key.as_ref());
        let at_peer = buf.create_string(at_peer.as_ref());
        let subscribed = topology::SubscribedToContract::create(
            &mut buf,
            &topology::SubscribedToContractArgs {
                requester: Some(requester),
                transaction: Some(transaction),
                key: Some(contract_key),
                contract_location,
                at_peer: Some(at_peer),
                at_peer_location,
                timestamp,
            },
        );

        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract_key),
                change_type: topology::ContractChangeType::SubscribedToContract,
                change: Some(subscribed.as_union_value()),
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn update_request_msg<'a>(
        transaction: impl AsRef<str>,
        contract: impl AsRef<str>,
        requester: impl AsRef<str>,
        target: impl AsRef<str>,
        timestamp: u64,
        contract_location: f64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();

        let transaction = buf.create_string(transaction.as_ref());
        let contract = buf.create_string(contract.as_ref());
        let requester = buf.create_string(requester.as_ref());
        let target = buf.create_string(target.as_ref());

        let update_req = topology::UpdateRequest::create(
            &mut buf,
            &topology::UpdateRequestArgs {
                transaction: Some(transaction),
                key: Some(contract),
                requester: Some(requester),
                target: Some(target),
                timestamp,
                contract_location,
            },
        );

        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract),
                change_type: topology::ContractChangeType::UpdateRequest,
                change: Some(update_req.as_union_value()),
            },
        );

        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn update_success_msg<'a>(
        transaction: impl AsRef<str>,
        contract: impl AsRef<str>,
        requester: impl AsRef<str>,
        target: impl AsRef<str>,
        timestamp: u64,
        contract_location: f64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();

        let transaction = buf.create_string(transaction.as_ref());
        let contract = buf.create_string(contract.as_ref());
        let requester = buf.create_string(requester.as_ref());
        let target = buf.create_string(target.as_ref());

        let update_success = topology::UpdateSuccess::create(
            &mut buf,
            &topology::UpdateSuccessArgs {
                transaction: Some(transaction),
                key: Some(contract),
                requester: Some(requester),
                target: Some(target),
                timestamp,
                contract_location,
            },
        );

        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract),
                change_type: topology::ContractChangeType::UpdateSuccess,
                change: Some(update_success.as_union_value()),
            },
        );

        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn update_failure_msg<'a>(
        transaction: impl AsRef<str>,
        contract: impl AsRef<str>,
        requester: impl AsRef<str>,
        target: impl AsRef<str>,
        timestamp: u64,
        contract_location: f64,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();

        let transaction = buf.create_string(transaction.as_ref());
        let contract = buf.create_string(contract.as_ref());
        let requester = buf.create_string(requester.as_ref());
        let target = buf.create_string(target.as_ref());

        let update_failure = topology::UpdateFailure::create(
            &mut buf,
            &topology::UpdateFailureArgs {
                transaction: Some(transaction),
                key: Some(contract),
                requester: Some(requester),
                target: Some(target),
                timestamp,
                contract_location,
            },
        );

        let msg = topology::ContractChange::create(
            &mut buf,
            &topology::ContractChangeArgs {
                contract_id: Some(contract),
                change_type: topology::ContractChangeType::UpdateFailure,
                change: Some(update_failure.as_union_value()),
            },
        );

        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }
}

impl PeerChange<'_> {
    pub fn current_state_msg<'a>(
        to: String,
        to_location: f64,
        connections: impl Iterator<Item = &'a (String, f64)>,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let to = buf.create_vector(to.as_bytes());
        let connections = connections
            .map(|(from, from_location)| {
                let from = Some(buf.create_vector(from.as_bytes()));
                topology::AddedConnection::create(
                    &mut buf,
                    &topology::AddedConnectionArgs {
                        transaction: None,
                        from,
                        from_location: *from_location,
                        to: Some(to),
                        to_location,
                    },
                )
            })
            .collect::<Vec<_>>();
        let connections = buf.create_vector(&connections);
        let msg = topology::PeerChange::create(
            &mut buf,
            &topology::PeerChangeArgs {
                change_type: topology::PeerChangeType::NONE,
                change: None,
                current_state: Some(connections),
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn added_connection_msg(
        transaction: Option<impl AsRef<str>>,
        (from, from_location): (String, f64),
        (to, to_location): (String, f64),
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let from = buf.create_vector(from.as_bytes());
        let to = buf.create_vector(to.as_bytes());
        let transaction = transaction.map(|t| buf.create_string(t.as_ref()));
        let add_conn = topology::AddedConnection::create(
            &mut buf,
            &topology::AddedConnectionArgs {
                transaction,
                from: Some(from),
                from_location,
                to: Some(to),
                to_location,
            },
        );
        let msg = topology::PeerChange::create(
            &mut buf,
            &topology::PeerChangeArgs {
                change_type: topology::PeerChangeType::AddedConnection,
                change: Some(add_conn.as_union_value()),
                current_state: None,
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }

    pub fn removed_connection_msg(at: String, from: String) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let at = buf.create_vector(&at.as_bytes());
        let from = buf.create_vector(&from.as_bytes());
        let remove_conn = topology::RemovedConnection::create(
            &mut buf,
            &topology::RemovedConnectionArgs {
                at: Some(at),
                from: Some(from),
            },
        );
        let msg = topology::PeerChange::create(
            &mut buf,
            &topology::PeerChangeArgs {
                change_type: topology::PeerChangeType::RemovedConnection,
                change: Some(remove_conn.as_union_value()),
                current_state: None,
            },
        );
        buf.finish_minimal(msg);
        buf.finished_data().to_vec()
    }
}

impl<'a> TryFromFbs<'a> for PeerChange<'a> {
    fn try_decode_fbs(buf: &'a [u8]) -> Result<Self, flatbuffers::InvalidFlatbuffer> {
        let req = flatbuffers::root::<topology::PeerChange>(&buf)?;
        match req.change_type() {
            topology::PeerChangeType::AddedConnection => {
                let req = req.change_as_added_connection().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "PeerChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::AddedConnection(req))
            }
            topology::PeerChangeType::RemovedConnection => {
                let req = req.change_as_removed_connection().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "PeerChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::RemovedConnection(req))
            }
            topology::PeerChangeType::Error => {
                let req = req.change_as_error().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "PeerChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::Error(req))
            }
            _ => unreachable!("PeerChangeType enum should be exhaustive"),
        }
    }
}

impl<'a> topology::ControllerResponse<'a> {
    pub fn into_fbs_bytes(result: Result<Option<String>, String>) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let (response_type, response) = match result {
            Ok(msg) => {
                let message = msg.as_ref().map(|s| builder.create_string(s));
                let response = topology::Ok::create(&mut builder, &topology::OkArgs { message });
                (topology::Response::Ok, response.as_union_value())
            }
            Err(msg) => {
                let message = Some(builder.create_string(&msg));
                let response =
                    topology::Error::create(&mut builder, &topology::ErrorArgs { message });
                (topology::Response::Error, response.as_union_value())
            }
        };
        let response = topology::ControllerResponse::create(
            &mut builder,
            &topology::ControllerResponseArgs {
                response_type,
                response: Some(response),
            },
        );
        builder.finish(response, None);
        builder.finished_data().to_vec()
    }
}

impl<'a> TryFromFbs<'a> for ContractChange<'a> {
    fn try_decode_fbs(buf: &'a [u8]) -> Result<Self, flatbuffers::InvalidFlatbuffer> {
        let req = flatbuffers::root::<topology::ContractChange>(&buf)?;
        match req.change_type() {
            topology::ContractChangeType::PutRequest => {
                let req = req.change_as_put_request().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::PutRequest(req))
            }
            topology::ContractChangeType::PutSuccess => {
                let req = req.change_as_put_success().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::PutSuccess(req))
            }
            topology::ContractChangeType::PutFailure => {
                let req = req.change_as_put_failure().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::PutFailure(req))
            }
            topology::ContractChangeType::BroadcastEmitted => {
                let req = req.change_as_broadcast_emitted().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::BroadcastEmitted(req))
            }
            topology::ContractChangeType::BroadcastReceived => {
                let req = req.change_as_broadcast_received().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::BroadcastReceived(req))
            }
            topology::ContractChangeType::GetContract => {
                let req = req.change_as_get_contract().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::GetContract(req))
            }
            topology::ContractChangeType::SubscribedToContract => {
                let req = req.change_as_subscribed_to_contract().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::SubscribedToContract(req))
            }
            topology::ContractChangeType::UpdateRequest => {
                let req = req.change_as_update_request().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::UpdateRequest(req))
            }
            topology::ContractChangeType::UpdateSuccess => {
                let req = req.change_as_update_success().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::UpdateSuccess(req))
            }

            topology::ContractChangeType::UpdateFailure => {
                let req = req.change_as_update_failure().ok_or_else(|| {
                    flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                        field: "change_type",
                        field_type: "ContractChangeType",
                        error_trace: Default::default(),
                    }
                })?;
                Ok(Self::UpdateFailure(req))
            }

            _ => unreachable!("ContractChangeType enum should be exhaustive"),
        }
    }
}
