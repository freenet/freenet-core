//! Temporal quantification.

use std::mem::MaybeUninit;

use chrono::{DateTime, Utc};

pub fn now() -> DateTime<Utc> {
    let mut uninit = MaybeUninit::<chrono::DateTime<Utc>>::uninit();
    let ptr = uninit.as_mut_ptr() as usize as i64;
    // eprintln!("{ptr} inside");
    unsafe {
        utc_now(crate::global::INSTANCE_ID, ptr);
        uninit.assume_init();
        std::mem::transmute(uninit)
    }
}

#[link(wasm_import_module = "locutus_time")]
extern "C" {
    fn utc_now(id: i64, ptr: i64);
}
