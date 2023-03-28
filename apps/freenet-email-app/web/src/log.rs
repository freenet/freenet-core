pub fn log(msg: impl AsRef<str>) {
    #[cfg(target_family = "wasm")]
    {
        web_sys::console::info_1(&serde_wasm_bindgen::to_value(&msg.as_ref()).unwrap());
    }
}

pub fn error(msg: impl AsRef<str>) {
    #[cfg(target_family = "wasm")]
    {
        web_sys::console::error_1(&serde_wasm_bindgen::to_value(&msg.as_ref()).unwrap());
    }
}
