#[no_mangle]
pub extern "C" fn time_func() {
    freenet_stdlib::log::info("trying to get time");
    let now = freenet_stdlib::time::now();
    freenet_stdlib::log::info(&format!("current time {now}"));
}
