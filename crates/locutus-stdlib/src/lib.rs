//! Standard library provided by the Freenet project to be able to write Locutus-compatible contracts.
pub mod buffer;
pub mod interface;

pub use locutus_macros::contract;

pub mod prelude {
    pub use crate::buffer::{Buffer, BufferBuilder, BufferMut, Error as BufferError};
    pub use crate::interface::*;
    pub use locutus_macros::contract;
}
