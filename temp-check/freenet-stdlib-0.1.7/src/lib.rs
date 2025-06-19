//! Standard library provided by the Freenet project to be able to write Locutus-compatible contracts.
mod code_hash;
#[cfg(feature = "unstable")]
pub mod contract_composition;
mod contract_interface;
mod delegate_interface;
pub(crate) mod global;
pub mod memory;
mod parameters;
mod versioning;

pub use contract_interface::encoding as typed_contract;

#[allow(dead_code, unused_imports, clippy::all)]
pub(crate) mod generated {
    mod client_request_generated;
    pub(crate) use client_request_generated::*;
    pub(crate) mod common_generated;
    pub(crate) use common_generated::*;
    mod host_response_generated;
    pub(crate) use host_response_generated::*;
}

pub(crate) mod common_generated {
    pub use super::generated::common_generated::*;
}

pub mod client_api;
#[cfg(feature = "contract")]
pub mod log;
#[cfg(feature = "contract")]
pub mod rand;
#[cfg(feature = "contract")]
pub mod time;

/// Locutus stdlib prelude.
pub mod prelude {
    pub use crate::code_hash::*;
    pub use crate::contract_interface::wasm_interface::ContractInterfaceResult;
    pub use crate::contract_interface::*;
    pub use crate::delegate_interface::wasm_interface::DelegateInterfaceResult;
    pub use crate::delegate_interface::*;
    pub use crate::parameters::*;
    pub use crate::typed_contract::{
        BincodeEncoder, Encoder, EncodingAdapter, JsonEncoder, RelatedContractsContainer,
    };
    pub use crate::versioning::*;
    pub use freenet_macros::{contract, delegate};

    pub use bincode;
    pub use blake3;
    pub use serde_json;
    pub use tracing;
    pub use tracing_subscriber;
}
