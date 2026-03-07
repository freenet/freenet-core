/// An instance ID, this is usually modified by the runtime when instancing a module.
pub(crate) static mut INSTANCE_ID: i64 = -1i64;

#[no_mangle]
extern "C" fn __frnt_set_id(id: i64) {
    unsafe {
        INSTANCE_ID = id;
    }
}
