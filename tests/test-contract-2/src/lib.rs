use locutus_stdlib::prelude::*;

#[no_mangle]
pub extern "C" fn time_func() {
    locutus_stdlib::log::info("trying to get time");
    let now = locutus_stdlib::time::now();
    locutus_stdlib::log::info(&format!("current time {now}"));
}
