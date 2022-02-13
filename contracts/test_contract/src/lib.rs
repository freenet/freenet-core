#[no_mangle]
pub extern "C" fn validate_value(ptr: *mut u8, len: i32) -> i32 {
    // eprintln!("accessing ptr: ({ptr:p}, {len})");
    let data = get_data(ptr, len as usize);
    // eprintln!("current data: {data:?}");
    unsafe {
        if *data.get_unchecked(0) == 1 && *data.get_unchecked(3) == 4 {
            // eprintln!("is valid");
            1
        } else {
            // eprintln!("is not valid");
            0
        }
    }
}

fn get_data(ptr: *mut u8, len: usize) -> &'static mut [u8] {
    let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
    slice
}
