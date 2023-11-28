#![allow(unused_imports, dead_code, clippy::all)]

#[cfg(feature = "trace")]
pub(crate) mod topology_generated;
pub use topology_generated::*;

use crate::node::PeerId;

pub trait TryFromFbs<'a>: Sized + 'a {
    fn try_decode_fbs(buf: &'a [u8]) -> Result<Self, flatbuffers::InvalidFlatbuffer>;
}

pub enum PeerChange<'a> {
    AddedConnection(topology::AddedConnection<'a>),
    RemovedConnection(topology::RemovedConnection<'a>),
    Error(topology::Error<'a>),
}

impl PeerChange<'_> {
    pub fn current_state_msg<'a>(
        to: PeerId,
        to_location: f64,
        connections: impl Iterator<Item = &'a (PeerId, f64)>,
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let to = buf.create_string(to.to_string().as_str());
        let connections = connections
            .map(|(from, from_location)| {
                let from = Some(buf.create_string(from.to_string().as_str()));
                topology::AddedConnection::create(
                    &mut buf,
                    &topology::AddedConnectionArgs {
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
        (from, from_location): (PeerId, f64),
        (to, to_location): (PeerId, f64),
    ) -> Vec<u8> {
        let mut buf = flatbuffers::FlatBufferBuilder::new();
        let from = Some(buf.create_string(from.to_string().as_str()));
        let to = Some(buf.create_string(to.to_string().as_str()));
        let add_conn = topology::AddedConnection::create(
            &mut buf,
            &topology::AddedConnectionArgs {
                from,
                from_location,
                to,
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
        let at = Some(buf.create_string(at.to_string().as_str()));
        let from = Some(buf.create_string(from.to_string().as_str()));
        let remove_conn = topology::RemovedConnection::create(
            &mut buf,
            &topology::RemovedConnectionArgs { at, from },
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
