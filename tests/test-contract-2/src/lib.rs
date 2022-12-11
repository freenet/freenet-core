use locutus_stdlib::prelude::*;

pub fn set_tra() {
    use locutus_stdlib::prelude::tracing_subscriber as tra;
    let _ = tra::fmt().with_env_filter("info").try_init();
}

#[no_mangle]
pub extern "C" fn time_func() {
    set_tra();
    tracing::info!("trying to get time");
    let now = locutus_stdlib::time::now();
    tracing::info!(%now, "time now");
    tracing::info!("done");
}
