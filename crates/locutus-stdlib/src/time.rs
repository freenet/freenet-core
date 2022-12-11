//! Temporal quantification.

use chrono::{DateTime, Utc};

#[derive(Debug)]
#[repr(C)]
struct Bytes {
    ptr: i64,
    length: i64,
    capacity: i64,
}

pub fn now() -> DateTime<Utc> {
    let Bytes {
        ptr,
        length,
        capacity,
    } = unsafe { &*(utc_now() as *mut Bytes) };
    dbg!(length);
    dbg!(capacity);
    dbg!(ptr);
    // tracing::info!("got ptr: {ptr}, l: {length}, cap: {capacity}");
    let bytes = unsafe { Vec::from_raw_parts(*ptr as _, *length as usize, *capacity as usize) };
    bincode::deserialize(&bytes).unwrap()
}

#[link(wasm_import_module = "locutus_time")]
extern "C" {
    fn utc_now() -> i64;
}
