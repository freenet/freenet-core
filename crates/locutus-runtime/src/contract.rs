use locutus_stdlib::prelude::{
    ContractCode, ContractInterfaceResult, ContractKey, Parameters, RelatedContracts, StateDelta,
    StateSummary, TryFromTsStd, UpdateData, UpdateModification, ValidateResult,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::{borrow::Borrow, fmt::Display, fs::File, io::Read, ops::Deref, path::Path, sync::Arc};
use wasmer::NativeFunc;

use crate::{ContractError, ContractExecError, RuntimeInnerError, RuntimeResult, WsApiError};

type FfiReturnTy = i64;

pub trait ContractRuntimeInterface {
    /// Verify that the state is valid, given the parameters. This will be used before a peer
    /// caches a new state.
    fn validate_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        related: RelatedContracts,
    ) -> RuntimeResult<ValidateResult>;

    /// Verify that a delta is valid - at least as much as possible. The goal is to prevent DDoS of
    /// a contract by sending a large number of invalid delta updates. This allows peers
    /// to verify a delta before forwarding it.
    fn validate_delta(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        delta: &StateDelta<'_>,
    ) -> RuntimeResult<bool>;

    /// Determine whether this delta is a valid update for this contract. If it is, return the modified state,
    /// else return error.
    ///
    /// The contract must be implemented in a way such that this function call is idempotent:
    /// - If the same `update_state` is applied twice to a value, then the second will be ignored.
    /// - Application of `update_state` is "order invariant", no matter what the order in which the values are
    ///   applied, the resulting value must be exactly the same.
    fn update_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        update_data: &[UpdateData<'_>],
    ) -> RuntimeResult<UpdateModification<'static>>;

    /// Generate a concise summary of a state that can be used to create deltas relative to this state.
    ///
    /// This allows flexible and efficient state synchronization between peers.
    fn summarize_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'static>>;

    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
    fn get_state_delta(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        delta_to: &StateSummary<'_>,
    ) -> RuntimeResult<StateDelta<'static>>;
}

/// Just as `locutus_stdlib::Contract` but with some convenience impl.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WrappedContract {
    #[serde(
        serialize_with = "WrappedContract::ser_contract_data",
        deserialize_with = "WrappedContract::deser_contract_data"
    )]
    pub(crate) data: Arc<ContractCode<'static>>,
    #[serde(
        serialize_with = "WrappedContract::ser_params",
        deserialize_with = "WrappedContract::deser_params"
    )]
    pub(crate) params: Parameters<'static>,
    pub(crate) key: ContractKey,
}

impl PartialEq for WrappedContract {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for WrappedContract {}

impl WrappedContract {
    pub fn new(data: Arc<ContractCode<'static>>, params: Parameters<'static>) -> WrappedContract {
        let key = ContractKey::from((&params, &*data));
        WrappedContract { data, params, key }
    }

    #[inline]
    pub fn key(&self) -> &ContractKey {
        &self.key
    }

    #[inline]
    pub fn code(&self) -> &Arc<ContractCode<'static>> {
        &self.data
    }

    #[inline]
    pub fn params(&self) -> &Parameters<'static> {
        &self.params
    }

    pub(crate) fn get_data_from_fs(path: &Path) -> Result<ContractCode<'static>, std::io::Error> {
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        Ok(ContractCode::from(contract_data))
    }

    fn ser_contract_data<S>(data: &Arc<ContractCode<'_>>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        data.serialize(ser)
    }

    fn deser_contract_data<'de, D>(_deser: D) -> Result<Arc<ContractCode<'static>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // let data: ContractCode<'de> = Deserialize::deserialize(deser)?;
        // Ok(Arc::new(data))
        todo!()
    }

    fn ser_params<S>(data: &Parameters<'_>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        data.serialize(ser)
    }

    fn deser_params<'de, D>(_deser: D) -> Result<Parameters<'static>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // let data: ContractCode<'de> = Deserialize::deserialize(deser)?;
        // Ok(Arc::new(data))
        todo!()
    }
}

impl<'a> TryFrom<(&'a Path, Parameters<'static>)> for WrappedContract {
    type Error = std::io::Error;
    fn try_from(data: (&'a Path, Parameters<'static>)) -> Result<Self, Self::Error> {
        let (path, params) = data;
        let data = Arc::new(Self::get_data_from_fs(path)?);
        Ok(WrappedContract::new(data, params))
    }
}

impl TryFromTsStd<&rmpv::Value> for WrappedContract {
    fn try_decode(value: &rmpv::Value) -> Result<Self, WsApiError> {
        let contract_map: HashMap<&str, &rmpv::Value> = match value.as_map() {
            Some(map_value) => HashMap::from_iter(
                map_value
                    .iter()
                    .map(|(key, val)| (key.as_str().unwrap(), val)),
            ),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding WrappedContract, input value is not a map".to_string(),
                })
            }
        };

        let key = match contract_map.get("key") {
            Some(key_value) => ContractKey::try_decode(*key_value).unwrap(),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding WrappedContract, key not found".to_string(),
                })
            }
        };

        let data = match contract_map.get("data") {
            Some(contract_data_value) => Arc::new(
                ContractCode::try_decode(*contract_data_value)
                    .unwrap()
                    .into_owned(),
            ),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding WrappedContract, data not found".to_string(),
                })
            }
        };

        let params = match contract_map.get("parameters") {
            Some(params_value) => Parameters::try_decode(*params_value).unwrap().into_owned(),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding WrappedContract, parameters not found".to_string(),
                })
            }
        };

        Ok(Self { data, params, key })
    }
}

impl TryInto<Vec<u8>> for WrappedContract {
    type Error = ContractError;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Arc::try_unwrap(self.data)
            .map(|r| r.into_bytes())
            .map_err(|_| RuntimeInnerError::UnwrapContract)
            .map_err(Into::into)
    }
}

impl Display for WrappedContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contract(")?;
        self.key.fmt(f)?;
        write!(f, ")")
    }
}

#[cfg(feature = "testing")]
impl<'a> arbitrary::Arbitrary<'a> for WrappedContract {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        use arbitrary::Arbitrary;
        let data: ContractCode = Arbitrary::arbitrary(u)?;
        let param_bytes: Vec<u8> = Arbitrary::arbitrary(u)?;
        let params = Parameters::from(param_bytes);
        let key = ContractKey::from((&params, &data));
        Ok(Self {
            data: Arc::new(data),
            params,
            key,
        })
    }
}

/// The state for a contract.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct WrappedState(
    #[serde(
        serialize_with = "WrappedState::ser_state",
        deserialize_with = "WrappedState::deser_state"
    )]
    Arc<Vec<u8>>,
);

impl WrappedState {
    pub fn new(bytes: Vec<u8>) -> Self {
        WrappedState(Arc::new(bytes))
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }

    fn ser_state<S>(data: &Arc<Vec<u8>>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::serialize(&**data, ser)
    }

    fn deser_state<'de, D>(deser: D) -> Result<Arc<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: Vec<u8> = serde_bytes::deserialize(deser)?;
        Ok(Arc::new(data))
    }
}

impl From<Vec<u8>> for WrappedState {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl TryFromTsStd<&rmpv::Value> for WrappedState {
    fn try_decode(value: &rmpv::Value) -> Result<Self, WsApiError> {
        let state = value.as_slice().unwrap().to_vec();
        Ok(WrappedState::from(state))
    }
}

impl AsRef<[u8]> for WrappedState {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for WrappedState {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<[u8]> for WrappedState {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for WrappedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data: String = if self.0.len() > 8 {
            let last_4 = self.0.len() - 4;
            self.0[..4]
                .iter()
                .map(|b| char::from(*b))
                .chain("...".chars())
                .chain(self.0[last_4..].iter().map(|b| char::from(*b)))
                .collect()
        } else {
            self.0.iter().copied().map(char::from).collect()
        };
        write!(f, "ContractState(data: [{}])", data)
    }
}

impl ContractRuntimeInterface for crate::Runtime {
    fn validate_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        related: RelatedContracts,
    ) -> RuntimeResult<ValidateResult> {
        let req_bytes = parameters.size() + state.size();
        let instance = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&instance)?;

        let mut param_buf = self.init_buf(&instance, parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, state)?;
        state_buf.write(state)?;
        let serialized = bincode::serialize(&related)?;
        let mut related_buf = self.init_buf(&instance, &serialized)?;
        related_buf.write(serialized)?;

        let validate_func: NativeFunc<(i64, i64, i64), FfiReturnTy> =
            instance.exports.get_native_function("validate_state")?;
        let is_valid = unsafe {
            ContractInterfaceResult::from_raw(
                validate_func.call(
                    param_buf.ptr() as i64,
                    state_buf.ptr() as i64,
                    related_buf.ptr() as i64,
                )?,
                &linear_mem,
            )
            .unwrap_validate_state_res(linear_mem)
            .map_err(Into::<ContractExecError>::into)?
        };
        Ok(is_valid)
    }

    fn validate_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        delta: &StateDelta<'a>,
    ) -> RuntimeResult<bool> {
        // todo: if we keep this hot in memory on next calls overwrite the buffer with new delta
        let req_bytes = parameters.size() + delta.size();
        let instance = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&instance)?;

        let mut param_buf = self.init_buf(&instance, parameters)?;
        param_buf.write(parameters)?;
        let mut delta_buf = self.init_buf(&instance, delta)?;
        delta_buf.write(delta)?;

        let validate_func: NativeFunc<(i64, i64), FfiReturnTy> =
            instance.exports.get_native_function("validate_delta")?;
        let is_valid = unsafe {
            ContractInterfaceResult::from_raw(
                validate_func.call(param_buf.ptr() as i64, delta_buf.ptr() as i64)?,
                &linear_mem,
            )
            .unwrap_validate_delta_res(linear_mem)
            .map_err(Into::<ContractExecError>::into)?
        };
        Ok(is_valid)
    }

    fn update_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        update_data: &[UpdateData<'_>],
    ) -> RuntimeResult<UpdateModification<'static>> {
        // todo: if we keep this hot in memory some things to take into account:
        //       - over subsequent requests state size may change
        //       - the delta may not be necessarily the same size
        let req_bytes =
            parameters.size() + state.size() + update_data.iter().map(|e| e.size()).sum::<usize>();
        let instance = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&instance)?;

        let mut param_buf = self.init_buf(&instance, parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, state)?;
        state_buf.write(state.clone())?;
        let serialized = bincode::serialize(update_data)?;
        let mut update_data_buf = self.init_buf(&instance, &serialized)?;
        update_data_buf.write(serialized)?;

        let validate_func: NativeFunc<(i64, i64, i64), FfiReturnTy> =
            instance.exports.get_native_function("update_state")?;
        let update_res = unsafe {
            ContractInterfaceResult::from_raw(
                validate_func.call(
                    param_buf.ptr() as i64,
                    state_buf.ptr() as i64,
                    update_data_buf.ptr() as i64,
                )?,
                &linear_mem,
            )
            .unwrap_update_state(linear_mem)
            .map_err(Into::<ContractExecError>::into)?
        };
        Ok(update_res)
    }

    fn summarize_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'static>> {
        let req_bytes = parameters.size() + state.size();
        let instance = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&instance)?;

        let mut param_buf = self.init_buf(&instance, parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, state)?;
        state_buf.write(state)?;

        let summary_func: NativeFunc<(i64, i64), FfiReturnTy> =
            instance.exports.get_native_function("summarize_state")?;

        let result = unsafe {
            let int_res = ContractInterfaceResult::from_raw(
                summary_func.call(param_buf.ptr() as i64, state_buf.ptr() as i64)?,
                &linear_mem,
            );
            int_res
                .unwrap_summarize_state(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };
        Ok(result)
    }

    fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        summary: &StateSummary<'a>,
    ) -> RuntimeResult<StateDelta<'static>> {
        let req_bytes = parameters.size() + state.size() + summary.size();
        let instance = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&instance)?;

        let mut param_buf = self.init_buf(&instance, parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, state)?;
        state_buf.write(state.clone())?;
        let mut summary_buf = self.init_buf(&instance, summary)?;
        summary_buf.write(summary)?;

        let get_state_delta_func: NativeFunc<(i64, i64, i64), FfiReturnTy> =
            instance.exports.get_native_function("get_state_delta")?;

        let result = unsafe {
            let int_res = {
                ContractInterfaceResult::from_raw(
                    get_state_delta_func.call(
                        param_buf.ptr() as i64,
                        state_buf.ptr() as i64,
                        summary_buf.ptr() as i64,
                    )?,
                    &linear_mem,
                )
            };
            int_res
                .unwrap_get_state_delta(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };
        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use locutus_stdlib::prelude::env_logger;

    use super::*;
    use crate::{
        contract::WrappedContract, secrets_store::SecretsStore, ContractContainer, ContractStore,
        Runtime, WasmAPIVersion,
    };
    use std::{path::PathBuf, sync::atomic::AtomicUsize};

    const TEST_CONTRACT_1: &str = "test_contract_1";
    static TEST_NO: AtomicUsize = AtomicUsize::new(0);

    fn test_dir() -> PathBuf {
        let test_dir = std::env::temp_dir().join("locutus-test").join(format!(
            "api-test-{}",
            TEST_NO.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        if !test_dir.exists() {
            std::fs::create_dir_all(&test_dir).unwrap();
        }
        test_dir
    }

    fn get_test_contract(name: &str) -> WrappedContract {
        const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let contracts = PathBuf::from(CONTRACTS_DIR);
        let mut dirs = contracts.ancestors();
        let path = dirs.nth(2).unwrap();
        let contract_path = path
            .join("tests")
            .join(name.replace('_', "-"))
            .join("build/locutus")
            .join(name)
            .with_extension("wasm");
        WrappedContract::try_from((&*contract_path, Parameters::from(vec![])))
            .expect("contract found")
    }

    fn set_up_test_contract(name: &str) -> RuntimeResult<(ContractStore, ContractKey)> {
        let _ = env_logger::try_init();
        let mut store = ContractStore::new(test_dir(), 10_000)?;
        let contract = ContractContainer::Wasm(WasmAPIVersion::V1(get_test_contract(name)));
        let key = contract.key();
        store.store_contract(contract)?;
        Ok((store, key))
    }

    #[test]
    fn validate_state() -> Result<(), Box<dyn std::error::Error>> {
        let (store, key) = set_up_test_contract(TEST_CONTRACT_1)?;
        let mut runtime = Runtime::build(store, SecretsStore::default(), false).unwrap();
        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi

        let is_valid = runtime.validate_state(
            &key,
            &Parameters::from([].as_ref()),
            &WrappedState::new(vec![1, 2, 3, 4]),
            Default::default(),
        )?;
        assert!(is_valid == ValidateResult::Valid);

        let not_valid = runtime.validate_state(
            &key,
            &Parameters::from([].as_ref()),
            &WrappedState::new(vec![1, 0, 0, 1]),
            Default::default(),
        )?;
        assert!(matches!(not_valid, ValidateResult::RequestRelated(_)));

        Ok(())
    }

    #[test]
    fn validate_delta() -> Result<(), Box<dyn std::error::Error>> {
        let (store, key) = set_up_test_contract(TEST_CONTRACT_1)?;
        let mut runtime = Runtime::build(store, SecretsStore::default(), false).unwrap();
        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi

        let is_valid = runtime.validate_delta(
            &key,
            &Parameters::from([].as_ref()),
            &StateDelta::from([1, 2, 3, 4].as_ref()),
        )?;
        assert!(is_valid);

        let not_valid = !runtime.validate_delta(
            &key,
            &Parameters::from([].as_ref()),
            &StateDelta::from([1, 0, 0, 1].as_ref()),
        )?;
        assert!(not_valid);

        Ok(())
    }

    #[test]
    fn update_state() -> Result<(), Box<dyn std::error::Error>> {
        let (store, key) = set_up_test_contract(TEST_CONTRACT_1)?;
        let mut runtime = Runtime::build(store, SecretsStore::default(), false).unwrap();
        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi

        let new_state = runtime
            .update_state(
                &key,
                &Parameters::from([].as_ref()),
                &WrappedState::new(vec![5, 2, 3]),
                &[StateDelta::from([4].as_ref()).into()],
            )?
            .unwrap_valid();
        assert!(new_state.as_ref().len() == 4);
        assert!(new_state.as_ref()[3] == 4);
        Ok(())
    }

    #[test]
    fn summarize_state() -> Result<(), Box<dyn std::error::Error>> {
        let (store, key) = set_up_test_contract(TEST_CONTRACT_1)?;
        let mut runtime = Runtime::build(store, SecretsStore::default(), false).unwrap();
        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi

        let summary = runtime.summarize_state(
            &key,
            &Parameters::from([].as_ref()),
            &WrappedState::new(vec![5, 2, 3, 4]),
        )?;
        assert_eq!(summary.as_ref(), &[5, 2, 3]);
        Ok(())
    }

    #[test]
    fn get_state_delta() -> Result<(), Box<dyn std::error::Error>> {
        let (store, key) = set_up_test_contract(TEST_CONTRACT_1)?;
        let mut runtime = Runtime::build(store, SecretsStore::default(), false).unwrap();
        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi

        let delta = runtime.get_state_delta(
            &key,
            &Parameters::from([].as_ref()),
            &WrappedState::new(vec![5, 2, 3, 4]),
            &StateSummary::from([2, 3].as_ref()),
        )?;
        assert!(delta.as_ref().len() == 1);
        assert!(delta.as_ref()[0] == 4);
        Ok(())
    }
}
