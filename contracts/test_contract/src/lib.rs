#[no_mangle]
pub extern "C" fn validate_value(ptr: i32, len: i32) -> i32 {
    let data = get_data(ptr, len);
    if data[0] == 1 {
        1
    } else {
        0
    }
}

fn get_data(ptr: i32, len: i32) -> &'static mut [u8] {
    let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len as usize) };
    slice
}
