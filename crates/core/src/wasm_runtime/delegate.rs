mod error;
mod execution;
mod interface;

pub use error::DelegateExecError;
pub(crate) use interface::DelegateRuntimeInterface;

#[cfg(all(test, feature = "wasmtime-backend"))]
mod test;
