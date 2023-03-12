//! Implementation of native API's exported and available in the WASM modules.

use dashmap::DashMap;
use once_cell::sync::Lazy;

/// This is a map of starting addresses of the instance memory space.
///
/// A hackish way of having the information necessary to compute the address
/// at which bytes must be written when calling host functions from the WASM modules.
pub(crate) static MEM_ADDR: Lazy<DashMap<InstanceId, i64>> = Lazy::new(DashMap::default);

type InstanceId = i64;

#[inline(always)]
fn compute_ptr<T>(ptr: i64, start_ptr: i64) -> *mut T {
    (start_ptr + ptr) as _
}

pub(crate) mod time {
    use super::*;
    use chrono::{DateTime, Utc as UtcOriginal};
    use wasmer::{Function, Imports};

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
        let start_ptr = *MEM_ADDR
            .get(&id)
            .expect("instance mem space not recorded")
            .value();
        let now = UtcOriginal::now();
        let ptr = compute_ptr::<DateTime<UtcOriginal>>(ptr, start_ptr);
        // eprintln!("{ptr:p} ({}) outside", ptr as i64);
        unsafe {
            let ptr = ptr as *mut DateTime<UtcOriginal>;
            ptr.write(now);
        };
    }
}
