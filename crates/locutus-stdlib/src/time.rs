//! Temporal quantification.

use std::mem::MaybeUninit;

use chrono::{DateTime, Utc};

pub fn now() -> DateTime<Utc> {
    let mut uninit = MaybeUninit::<chrono::DateTime<Utc>>::uninit();
    let ptr = uninit.as_mut_ptr() as usize as i64;
    unsafe {
        __loc__time__utc_now(crate::global::INSTANCE_ID, ptr);
        uninit.assume_init();
        std::mem::transmute(uninit)
    }
}

#[link(wasm_import_module = "locutus_time")]
extern "C" {
    #[doc(hidden)]
    fn __loc__time__utc_now(id: i64, ptr: i64);
}
