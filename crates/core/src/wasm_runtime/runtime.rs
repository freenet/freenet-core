use super::{
    contract_store::ContractStore, delegate_store::DelegateStore, error::RuntimeInnerError,
    native_api, secrets_store::SecretsStore, RuntimeResult,
};
use freenet_stdlib::{
    memory::{
        buf::{BufferBuilder, BufferMut},
        WasmLinearMem,
    },
    prelude::*,
};
use std::{collections::HashMap, sync::atomic::AtomicI64};
use wasmer::{
    imports, Bytes, CompilerConfig, Imports, Instance, Memory, MemoryType, Module, Store,
    TypedFunction,
};
use wasmer_middlewares::metering::{get_remaining_points, MeteringPoints};

static INSTANCE_ID: AtomicI64 = AtomicI64::new(0);

pub(super) struct RunningInstance {
    pub id: i64,
    pub instance: Instance,
}

impl Drop for RunningInstance {
    fn drop(&mut self) {
        let _ = native_api::MEM_ADDR.remove(&self.id);
    }
}

pub(super) struct InstanceInfo {
    pub start_ptr: i64,
    key: Key,
}

impl InstanceInfo {
    pub fn key(&self) -> String {
        match &self.key {
            Key::Contract(k) => k.encode(),
            Key::Delegate(k) => k.encode(),
        }
    }
}

enum Key {
    Contract(ContractInstanceId),
    Delegate(DelegateKey),
}

impl RunningInstance {
    fn new(rt: &mut Runtime, instance: Instance, key: Key) -> RuntimeResult<Self> {
        let memory = rt
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
        let set_id: TypedFunction<i64, ()> = instance
            .exports
            .get_typed_function(&rt.wasm_store, "__frnt_set_id")
            .unwrap();
        let id = INSTANCE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        set_id.call(&mut rt.wasm_store, id).unwrap();
        let ptr = memory.view(&rt.wasm_store).data_ptr() as i64;
        native_api::MEM_ADDR.insert(
            id,
            InstanceInfo {
                start_ptr: ptr,
                key,
            },
        );
        Ok(Self { instance, id })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ContractExecError {
    #[error(transparent)]
    ContractError(#[from] ContractError),

    #[error("Attempted to perform a put for an already put contract ({0}), use update instead")]
    DoublePut(ContractKey),

    #[error("insufficient memory, needed {req} bytes but had {free} bytes")]
    InsufficientMemory { req: usize, free: usize },

    #[error("could not cast array length of {0} to max size (i32::MAX)")]
    InvalidArrayLength(usize),

    #[error("unexpected result from contract interface")]
    UnexpectedResult,

    #[error("The operation ran out of gas. This might be caused by an infinite loop or an inefficient computation.")]
    OutOfGas,
}

pub struct Runtime {
    /// Working memory store used by the inner engine
    pub(super) wasm_store: Store,
    /// includes all the necessary imports to interact with the native runtime environment
    pub(super) top_level_imports: Imports,
    /// assigned growable host memory
    pub(super) host_memory: Option<Memory>,

    pub(super) secret_store: SecretsStore,
    pub(super) delegate_store: DelegateStore,
    /// loaded delegate modules
    pub(super) delegate_modules: HashMap<DelegateKey, Module>,

    /// Local contract storage.
    pub(crate) contract_store: ContractStore,
    /// loaded contract modules
    pub(super) contract_modules: HashMap<ContractKey, Module>,
}

impl Runtime {
    pub fn build(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
    ) -> RuntimeResult<Self> {
        let mut store = Self::instance_store();
        let (host_memory, mut top_level_imports) = if host_mem {
            let mem = Self::instance_host_mem(&mut store)?;
            let imports = imports! {
                "env" => {
                    "memory" =>  mem.clone(),
                },
            };
            (Some(mem), imports)
        } else {
            (None, imports! {})
        };
        native_api::log::prepare_export(&mut store, &mut top_level_imports);
        native_api::rand::prepare_export(&mut store, &mut top_level_imports);
        native_api::time::prepare_export(&mut store, &mut top_level_imports);

        Ok(Self {
            wasm_store: store,
            top_level_imports,
            host_memory,

            secret_store,
            delegate_store,
            contract_modules: HashMap::new(),

            contract_store,
            delegate_modules: HashMap::new(),
        })
    }

    pub(super) fn init_buf<T>(&mut self, instance: &Instance, data: T) -> RuntimeResult<BufferMut>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        let initiate_buffer: TypedFunction<u32, i64> = instance
            .exports
            .get_typed_function(&self.wasm_store, "__frnt__initiate_buffer")?;
        let builder_ptr = initiate_buffer.call(&mut self.wasm_store, data.len() as u32)?;
        let linear_mem = self.linear_mem(instance)?;
        unsafe {
            Ok(BufferMut::from_ptr(
                builder_ptr as *mut BufferBuilder,
                linear_mem,
            ))
        }
    }

    pub(super) fn linear_mem(&self, instance: &Instance) -> RuntimeResult<WasmLinearMem> {
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?
            .view(&self.wasm_store);
        Ok(unsafe { WasmLinearMem::new(memory.data_ptr() as *const _, memory.data_size()) })
    }

    pub(super) fn prepare_contract_call(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        let module = if let Some(module) = self.contract_modules.get(key) {
            module
        } else {
            let contract = self
                .contract_store
                .fetch_contract(key, parameters)
                .ok_or_else(|| RuntimeInnerError::ContractNotFound(*key))?;
            let module = match contract {
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => {
                    Module::new(&self.wasm_store, contract_v1.code().data())?
                }
                _ => unimplemented!(),
            };
            self.contract_modules.insert(*key, module);
            self.contract_modules.get(key).unwrap()
        }
        .clone();
        let instance = self.prepare_instance(&module)?;
        self.set_instance_mem(req_bytes, &instance)?;
        RunningInstance::new(self, instance, Key::Contract(*key.id()))
    }

    pub(super) fn prepare_delegate_call(
        &mut self,
        params: &Parameters,
        key: &DelegateKey,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        let module = if let Some(module) = self.delegate_modules.get(key) {
            module
        } else {
            let delegate = self
                .delegate_store
                .fetch_delegate(key, params)
                .ok_or_else(|| RuntimeInnerError::DelegateNotFound(key.clone()))?;
            let module = Module::new(&self.wasm_store, delegate.code().as_ref())?;
            self.delegate_modules.insert(key.clone(), module);
            self.delegate_modules.get(key).unwrap()
        }
        .clone();
        let instance = self.prepare_instance(&module)?;
        self.set_instance_mem(req_bytes, &instance)?;
        RunningInstance::new(self, instance, Key::Delegate(key.clone()))
    }

    fn set_instance_mem(&mut self, req_bytes: usize, instance: &Instance) -> RuntimeResult<()> {
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
        let req_pages: wasmer::Pages = Bytes::from(req_bytes).try_into().unwrap();
        if memory.view(&self.wasm_store).size() < req_pages {
            if let Err(err) = memory.grow(&mut self.wasm_store, req_pages) {
                tracing::error!("wasm runtime failed with memory error: {err}");
                return Err(ContractExecError::InsufficientMemory {
                    req: (req_pages.0 as usize * wasmer::WASM_PAGE_SIZE),
                    free: (memory.view(&self.wasm_store).size().0 as usize
                        * wasmer::WASM_PAGE_SIZE),
                }
                .into());
            }
        }
        Ok(())
    }

    fn instance_host_mem(store: &mut Store) -> RuntimeResult<Memory> {
        // todo: max memory assigned for this runtime
        Ok(Memory::new(store, MemoryType::new(20u32, None, false))?)
    }

    fn prepare_instance(&mut self, module: &Module) -> RuntimeResult<Instance> {
        Ok(Instance::new(
            &mut self.wasm_store,
            module,
            &self.top_level_imports,
        )?)
    }

    fn instance_store() -> Store {
        use std::sync::Arc;
        use wasmer::wasmparser::Operator;
        use wasmer::Cranelift;
        use wasmer_middlewares::Metering;

        fn get_cpu_cycles_per_second() -> (u64, f64) {
            // Assumed CPU speed for cost calculations (3.0 GHz)
            const DEFAULT_CPU_CYCLES_PER_SECOND: u64 = 3_000_000_000;
            // Additional buffer to account for varying CPU speeds
            const SAFETY_MARGIN: f64 = 0.2;
            if let Some(cpu) = option_env!("CPU_CYCLES_PER_SECOND") {
                (parse_cpu_cycles_per_second(cpu), 0.0)
            } else {
                get_cpu_cycles_per_second_runtime()
                    .map(|x| (x, 0.0))
                    .unwrap_or((DEFAULT_CPU_CYCLES_PER_SECOND, SAFETY_MARGIN))
            }
        }

        // Maximum allowed execution time for WASM code
        const MAX_EXECUTION_SECONDS: f64 = 5.0;

        let (cpu_cycles_per_sec, safety_margin) = get_cpu_cycles_per_second();

        // Calculate total allowed cycles including safety margin
        let max_cycles: u64 =
            (MAX_EXECUTION_SECONDS * cpu_cycles_per_sec as f64 * (1.0 + safety_margin)) as u64;

        // Cost function that assigns cycle costs to different WASM operations.
        // Costs are rough estimates based on typical CPU cycles:
        // - Control flow: ~10-25 cycles (with pipeline stalls) -> cost 25
        let operation_cost = |operator: &Operator| -> u64 {
            match operator {
                // Control flow operations
                Operator::Loop { .. } => 25,

                // Default cost for all other operations
                _ => 1,
            }
        };

        let metering = Arc::new(Metering::new(max_cycles, operation_cost));
        let mut compiler_config = Cranelift::default();
        compiler_config.push_middleware(metering);

        let engine = wasmer::EngineBuilder::new(compiler_config).engine();

        Store::new(&engine)
    }

    pub(crate) fn handle_contract_error<T>(
        &mut self,
        error: wasmer::RuntimeError,
        instance: &wasmer::Instance,
        function_name: &str,
    ) -> RuntimeResult<T> {
        let remaining_points = get_remaining_points(&mut self.wasm_store, instance);
        match remaining_points {
            MeteringPoints::Remaining(..) => {
                tracing::error!("Error while calling {}: {:?}", function_name, error);
                Err(error.into())
            }
            MeteringPoints::Exhausted => {
                tracing::error!(
                    "{} ran out of gas, not enough points remaining",
                    function_name
                );
                Err(ContractExecError::OutOfGas.into())
            }
        }
    }
}

const fn parse_cpu_cycles_per_second(cpu: &str) -> u64 {
    let bytes = cpu.as_bytes();
    let mut result = 0u64;
    let mut i = 0;
    while i < bytes.len() {
        result = result * 10 + (bytes[i] - b'0') as u64;
        i += 1;
    }
    result
}

#[cfg(target_os = "macos")]
fn get_cpu_cycles_per_second_runtime() -> Option<u64> {
    use std::process::Command;

    fn parse_sysctl_output(output: &str) -> Option<u64> {
        let parts: Vec<&str> = output.split(':').collect();
        if parts.len() == 2 {
            if let Ok(freq) = parts[1].trim().parse() {
                return Some(freq);
            }
        }
        None
    }

    let output = Command::new("sysctl")
        .arg("hw.cpufrequency")
        .output()
        .ok()?;

    if output.status.success() {
        let output_str = String::from_utf8_lossy(&output.stdout);
        println!("sysctl output (hw.cpufrequency): {}", output_str); // Debugging information

        if let Some(freq) = parse_sysctl_output(&output_str) {
            return Some(freq);
        }
    } else {
        eprintln!("sysctl command failed with status: {}", output.status);
    }

    // Fallback to hw.tbfrequency if hw.cpufrequency is not available
    let output = Command::new("sysctl").arg("hw.tbfrequency").output().ok()?;

    if output.status.success() {
        let output_str = String::from_utf8_lossy(&output.stdout);
        println!("sysctl output (hw.tbfrequency): {}", output_str); // Debugging information

        if let Some(freq) = parse_sysctl_output(&output_str) {
            return Some(freq);
        }
    } else {
        eprintln!("sysctl command failed with status: {}", output.status);
    }
    None
}

#[cfg(target_os = "linux")]
fn get_cpu_cycles_per_second_runtime() -> Option<u64> {
    use std::fs::File;
    use std::io::{self, BufRead};

    if let Ok(file) = File::open("/proc/cpuinfo") {
        for line in io::BufReader::new(file).lines() {
            if let Ok(line) = line {
                if line.starts_with("cpu MHz") {
                    let parts: Vec<&str> = line.split(':').collect();
                    if parts.len() == 2 {
                        let mhz: f64 = parts[1].trim().parse().ok()?;
                        return (mhz * 1_000_000.0) as u64;
                    }
                }
            }
        }
    }
    None
}

#[cfg(target_os = "windows")]
fn get_cpu_cycles_per_second_runtime() -> Option<u64> {
    use serde::Deserialize;
    use wmi::{COMLibrary, Variant, WMIConnection};

    #[derive(Deserialize, Debug)]
    struct Win32_Processor {
        #[serde(rename = "MaxClockSpeed")]
        max_clock_speed: u64,
    }

    let com_con = COMLibrary::new().ok()?;
    let wmi_con = WMIConnection::new(com_con.into()).ok()?;

    let results: Vec<Win32_Processor> = wmi_con
        .raw_query("SELECT MaxClockSpeed FROM Win32_Processor")
        .ok()?;

    if let Some(cpu) = results.first() {
        return Some(cpu.max_clock_speed * 1_000_000); // Convert MHz to Hz
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpu_cycles_per_second() {
        assert_eq!(parse_cpu_cycles_per_second("3000000000"), 3_000_000_000);
        assert_eq!(parse_cpu_cycles_per_second("2500000000"), 2_500_000_000);
        assert_eq!(parse_cpu_cycles_per_second("2000000000"), 2_000_000_000);
        assert_eq!(parse_cpu_cycles_per_second("1000000000"), 1_000_000_000);
    }
}
