#![allow(unused)] // FIXME: remove this attr

use crate::{
    conn_manager::{ConnectionManager, Transport},
    message::{ProbeRequest, ProbeResponse},
};

pub(crate) struct ProbeProtocol<CM> {
    // TODO: maybe both probe and ring proto should not hold a copy of connection manager
    // right now this is a transliteration of Kotlin code where this is easy to do
    // due to GC + cheap ref but may be limiting here and will need to refactor probably
    conn_manager: CM,
}

impl<CM, T> ProbeProtocol<CM>
where
    CM: ConnectionManager<Transport = T>,
    T: Transport,
{
    const MAXIMUM_HOPS_TO_LIVE: usize = 10;

    // TODO: think if the uniqueness constraint check is required in the Rust impl
    // probably not since the the ownership and move semantics will ensure this
    // but keep in the back of mind when this is more complete and have to clean up
    // in case we end up cloning / ref counting
    // re: init { cm.assertUnique(this::class) }

    pub fn new(conn_manager: CM) -> Self {
        // conn_manager.listen(reaction, message);
        Self { conn_manager }
    }

    // TODO: this must be really async since it will perform net I/O
    pub fn probe(&mut self, probe_req: ProbeRequest) -> ProbeResponse {
        todo!()
    }
}
