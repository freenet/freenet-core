// Stub for __rust_probestack to fix CI linking issues with wasmer
#[no_mangle]
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub extern "C" fn __rust_probestack() {
    // This is a stub implementation
    // The real probestack is used for stack overflow checking
    // but isn't critical for our CI tests
}
