#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        #[cfg(not(feature = "contract"))]
        tracing::info!($($arg)*);
        #[cfg(feature = "contract")]
        ::freenet_stdlib::log::info(&format!($($arg)*));
    };
}

pub fn info(msg: &str) {
    let ptr = msg.as_ptr() as _;
    unsafe {
        __frnt__logger__info(crate::global::INSTANCE_ID, ptr, msg.len() as _);
    }
}

#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_log")]
extern "C" {
    #[doc(hidden)]
    fn __frnt__logger__info(id: i64, ptr: i64, len: i32);
}

#[cfg(not(target_family = "wasm"))]
#[allow(non_snake_case)]
unsafe fn __frnt__logger__info(_id: i64, _ptr: i64, _len: i32) {
    // Stub implementation for non-WASM targets (e.g., Windows native compilation)
    // These contracts are meant to run only in WASM runtime
}
