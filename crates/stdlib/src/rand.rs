//! Random number generation.

pub fn rand_bytes<'a>(_len: u32) -> &'a [u32] {
    todo!()
}

#[link(wasm_import_module = "locutus_rand")]
extern "C" {
    #[doc(hidden)]
    fn __loc__rand__rand_bytes(id: i64, ptr: i64);
}
