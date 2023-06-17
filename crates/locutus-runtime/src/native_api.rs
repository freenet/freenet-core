//! Implementation of native API's exported and available in the WASM modules.

use dashmap::DashMap;
use once_cell::sync::Lazy;
use wasmer::{Function, Imports};

use crate::runtime::InstanceInfo;

/// This is a map of starting addresses of the instance memory space.
///
/// A hackish way of having the information necessary to compute the address
/// at which bytes must be written when calling host functions from the WASM modules.
pub(crate) static MEM_ADDR: Lazy<DashMap<InstanceId, InstanceInfo>> = Lazy::new(DashMap::default);

type InstanceId = i64;

#[inline(always)]
fn compute_ptr<T>(ptr: i64, start_ptr: i64) -> *mut T {
    let start_ptr: *mut T = start_ptr as _;
    unsafe { start_ptr.offset(ptr as _) }
}

pub(crate) mod time {
    use super::*;
    use chrono::{DateTime, Utc as UtcOriginal};

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let utc_now = Function::new_typed(store, utc_now);
        imports.register_namespace(
            "locutus_time",
            [("__loc__time__utc_now".to_owned(), utc_now.into())],
        );
    }

    fn utc_now(id: i64, ptr: i64) {
        if id == -1 {
            panic!("unset module id");
        }
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let now = UtcOriginal::now();
        let ptr = compute_ptr::<DateTime<UtcOriginal>>(ptr, info.start_ptr);
        // eprintln!("{ptr:p} ({}) outside", ptr as i64);
        unsafe {
            let ptr = ptr as *mut DateTime<UtcOriginal>;
            ptr.write(now);
        };
    }
}

pub(crate) mod log {
    use wasmer::Function;

    use super::*;

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let utc_now = Function::new_typed(store, info);
        imports.register_namespace(
            "locutus_logger",
            [("__loc__logger__info".to_owned(), utc_now.into())],
        );
    }

    // TODO: this API right now is just a patch, ideally we want to impl a tracing subscriber
    // that can be used in wasm and that under the hood will just pass data to the host via
    // functions like this in a structured way
    fn info(id: i64, ptr: i64, len: i32) {
        if id == -1 {
            panic!("unset module id");
        }
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let ptr = compute_ptr::<u8>(ptr, info.start_ptr);
        let msg =
            unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as _)) };
        tracing::info!(target: "contract", key = %info.value().key(), "{msg}");
    }
}
