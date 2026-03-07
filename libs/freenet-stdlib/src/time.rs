//! Temporal quantification.

use std::mem::MaybeUninit;

use chrono::{DateTime, Utc};

pub fn now() -> DateTime<Utc> {
    let mut uninit = MaybeUninit::<chrono::DateTime<Utc>>::uninit();
    let ptr = uninit.as_mut_ptr() as usize as i64;
    unsafe {
        __frnt__time__utc_now(crate::global::INSTANCE_ID, ptr);
        uninit.assume_init();
        std::mem::transmute(uninit)
    }
}

#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_time")]
extern "C" {
    #[doc(hidden)]
    fn __frnt__time__utc_now(id: i64, ptr: i64);
}

#[cfg(not(target_family = "wasm"))]
#[allow(non_snake_case)]
unsafe fn __frnt__time__utc_now(_id: i64, _ptr: i64) {
    // Stub implementation for non-WASM targets (e.g., Windows native compilation)
    // These contracts are meant to run only in WASM runtime
}
