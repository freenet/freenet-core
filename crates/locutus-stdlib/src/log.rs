pub fn info(msg: &str) {
    let ptr = msg.as_ptr() as _;
    unsafe {
        __loc__logger__info(crate::global::INSTANCE_ID, ptr, msg.len() as _);
    }
}

#[link(wasm_import_module = "locutus_logger")]
extern "C" {
    #[doc(hidden)]
    fn __loc__logger__info(id: i64, ptr: i64, len: i32);
}
