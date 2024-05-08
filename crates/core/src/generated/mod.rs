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
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let transaction = buf.create_string(transaction.as_str());
        let contract = buf.create_string(contract.as_str());
        let requester = buf.create_string(requester.as_str());
        let target = buf.create_string(target.to_string().as_str());
        let put_success = topology::PutSuccess::create(
            &mut buf,
            &topology::PutSuccessArgs {
                transaction: Some(transaction),
                key: Some(contract),
                requester: Some(requester),
                target: Some(target),
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
}

impl PeerChange<'_> {
    pub fn current_state_msg<'a>(
        to: PeerId,
        to_location: f64,
        connections: impl Iterator<Item = &'a (PeerId, f64)>,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let to = buf.create_vector(&bincode::serialize(&to).unwrap());
        let connections = connections
            .map(|(from, from_location)| {
                let from = Some(buf.create_vector(&bincode::serialize(from).unwrap()));
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
        (from, from_location): (PeerId, f64),
        (to, to_location): (PeerId, f64),
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let from = buf.create_vector(&bincode::serialize(&from).unwrap());
        let to = buf.create_vector(&bincode::serialize(&to).unwrap());
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

    pub fn removed_connection_msg(at: PeerId, from: PeerId) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let at = buf.create_vector(&bincode::serialize(&at).unwrap());
        let from = buf.create_vector(&bincode::serialize(&from).unwrap());
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
            _ => unreachable!(),
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
            _ => unreachable!(),
        }
    }
}
