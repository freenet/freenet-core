//! Random number generation.
use std::cell::RefCell;

thread_local! {
    static SMALL_BUF: RefCell<[u8; 512]> = const { RefCell::new([0u8; 512]) };
    static LARGE_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Get the specified number of random bytes.
pub fn rand_bytes(number: u32) -> Vec<u8> {
    const MAX_KEY_SIZE: u32 = 512;

    if number <= MAX_KEY_SIZE {
        SMALL_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            unsafe {
                __frnt__rand__rand_bytes(crate::global::INSTANCE_ID, buf.as_mut_ptr() as _, number);
            }
            buf[..number as usize].to_vec()
        })
    } else {
        LARGE_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            let len = number as usize;
            if buf.len() < len {
                buf.resize(len, 0);
            }
            unsafe {
                __frnt__rand__rand_bytes(crate::global::INSTANCE_ID, buf.as_mut_ptr() as _, number);
            }
            buf[..len].to_vec()
        })
    }
}

#[link(wasm_import_module = "freenet_rand")]
extern "C" {
    #[doc(hidden)]
    fn __frnt__rand__rand_bytes(id: i64, ptr: i64, len: u32);
}
