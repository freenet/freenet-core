//! Implementation of native API's exported and available in the WASM modules.

struct Env {
    /// Untyped data passed from host to guest.
    data: Vec<u8>,
    ptr: Bytes,
}

impl Env {
    fn new() -> Self {
        Self {
            data: vec![0; 1000],
            ptr: Bytes::default(),
        }
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Bytes {
    ptr: i64,
    length: i64,
    capacity: i64,
}

pub(crate) mod time {
    use chrono::Utc as UtcOriginal;
    use wasmer::{Function, FunctionEnv, FunctionEnvMut, Imports};

    use super::*;

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let env = FunctionEnv::new(store, Env::new());
        let utc_now = Function::new_typed_with_env(store, &env, utc_now);
        imports.register_namespace("locutus_time", [("utc_now".to_owned(), utc_now.into())]);
        // imports.define("env", "__locutus__time__utc_now", utc_now);
    }

    fn utc_now(mut env: FunctionEnvMut<Env>) -> i64 {
        let env = env.data_mut();
        let data = &mut env.data;
        data.clear();
        let now = UtcOriginal::now();
        let mut ser = bincode::serialize(&now).unwrap();
        data.append(&mut ser);

        let length = ser.len() as i64;
        let capacity = ser.capacity() as i64;
        let ptr = data.as_ptr() as i64;
        env.ptr.ptr = ptr;
        env.ptr.length = length;
        env.ptr.capacity = capacity;
        &mut env.ptr as *mut _ as i64
    }
}
