//! Interface and related utilities for interaction with the compiled WASM contracts.
//! Contracts have an isomorphic interface which partially maps to this interface,
//! allowing interaction between the runtime and the contracts themselves.
//!
//! This abstraction layer shouldn't leak beyond the contract handler.

use std::{
    borrow::{Borrow, Cow},
    fmt::Display,
    hash::Hasher,
    io::{Cursor, Read},
    ops::{Deref, DerefMut},
    str::FromStr,
};

use blake2::{Blake2s256, Digest};
use byteorder::LittleEndian;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

const CONTRACT_KEY_SIZE: usize = 32;

/// Type of errors during interaction with a contract.
#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("invalid contract update")]
    InvalidUpdate,
    #[error("trying to read an invalid state")]
    InvalidState,
    #[error("trying to read an invalid delta")]
    InvalidDelta,
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[doc(hidden)]
#[repr(i32)]
pub enum UpdateResult {
    ValidUpdate = 0i32,
    ValidNoChange = 1i32,
    Invalid = 2i32,
}

impl From<ContractError> for UpdateResult {
    fn from(_err: ContractError) -> Self {
        UpdateResult::Invalid
    }
}

impl TryFrom<i32> for UpdateResult {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ValidUpdate),
            1 => Ok(Self::ValidNoChange),
            2 => Ok(Self::Invalid),
            _ => Err(()),
        }
    }
}

/// Result of a `State` update.
pub enum UpdateModification {
    ValidUpdate(State<'static>),
    NoChange,
}

impl UpdateModification {
    pub fn unwrap_valid(self) -> State<'static> {
        match self {
            Self::ValidUpdate(s) => s,
            _ => panic!("failed unwrapping state in modification"),
        }
    }
}

// ANCHOR: contractifce
/// Trait to implement for the contract building.
///
/// Contains all necessary methods to interact with the contract.
///
///# Examples
///
/// Implementing `ContractInterface` on a type:
///
/// ```
/// impl ContractInterface for Contract {
///     fn validate_state(_parameters: Parameters<'static>, _state: State<'static>) -> bool {
///         true
///     }
///
///     fn validate_delta(_parameters: Parameters<'static>, _delta: StateDelta<'static>) -> bool {
///         true
///     }
///
///     fn update_state(
///         _parameters: Parameters<'static>,
///         state: State<'static>,
///         _delta: StateDelta<'static>,
///     ) -> Result<UpdateModification, ContractError> {
///         Ok(UpdateModification::ValidUpdate(state))
///     }
///
///     fn summarize_state(
///         _parameters: Parameters<'static>,
///         _state: State<'static>,
///     ) -> StateSummary<'static> {
///         StateSummary::from(vec![])
///     }
///
///     fn get_state_delta(
///         _parameters: Parameters<'static>,
///         _state: State<'static>,
///         _summary: StateSummary<'static>,
///     ) -> StateDelta<'static> {
///         StateDelta::from(vec![])
///     }
/// }
/// ```
pub trait ContractInterface {
    /// Verify that the state is valid, given the parameters.
    fn validate_state(parameters: Parameters<'static>, state: State<'static>) -> bool;

    /// Verify that a delta is valid if possible, returns false if and only delta is
    /// definitely invalid, true otherwise.
    fn validate_delta(parameters: Parameters<'static>, delta: StateDelta<'static>) -> bool;

    /// Update the state to account for the state_delta, assuming it is valid.
    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        delta: StateDelta<'static>,
    ) -> Result<UpdateModification, ContractError>;

    /// Generate a concise summary of a state that can be used to create deltas
    /// relative to this state.
    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> StateSummary<'static>;

    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
    fn get_state_delta(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> StateDelta<'static>;
}
// ANCHOR_END: contractifce

/// A complete contract specification requires a `parameters` section
/// and a `contract` section.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract<'a> {
    pub parameters: Parameters<'a>,
    pub data: ContractCode<'a>,
    key: ContractKey,
}

impl<'a> Contract<'a> {
    /// Returns a contract from [contract code](ContractCode) and given [parameters](Parameters).
    pub fn new(contract: ContractCode<'a>, parameters: Parameters<'a>) -> Contract<'a> {
        let key = ContractKey::from((&parameters, &contract));
        Contract {
            parameters,
            data: contract,
            key,
        }
    }

    /// Key portion of the specification.
    pub fn key(&self) -> &ContractKey {
        &self.key
    }

    /// Code portion of the specification.
    pub fn into_code(self) -> ContractCode<'a> {
        self.data
    }
}

impl TryFrom<Vec<u8>> for Contract<'static> {
    type Error = std::io::Error;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        use byteorder::ReadBytesExt;
        let mut reader = Cursor::new(data);

        let params_len = reader.read_u64::<LittleEndian>()?;
        let mut params_buf = vec![0; params_len as usize];
        reader.read_exact(&mut params_buf)?;
        let parameters = Parameters::from(params_buf);

        let contract_len = reader.read_u64::<LittleEndian>()?;
        let mut contract_buf = vec![0; contract_len as usize];
        reader.read_exact(&mut contract_buf)?;
        let contract = ContractCode::from(contract_buf);

        let key = ContractKey::from((&parameters, &contract));

        Ok(Contract {
            parameters,
            data: contract,
            key,
        })
    }
}

impl PartialEq for Contract<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Contract<'_> {}

impl std::fmt::Display for Contract<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ContractSpec( key: ")?;
        internal_fmt_key(&self.key.id.0, f)?;
        let data: String = if self.data.data.len() > 8 {
            self.data.data[..4]
                .iter()
                .map(|b| char::from(*b))
                .chain("...".chars())
                .chain(self.data.data[4..].iter().map(|b| char::from(*b)))
                .collect()
        } else {
            self.data.data.iter().copied().map(char::from).collect()
        };
        write!(f, ", data: [{}])", data)
    }
}

#[cfg(any(test, feature = "testing"))]
impl<'a> arbitrary::Arbitrary<'a> for Contract<'static> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let contract: ContractCode = u.arbitrary()?;
        let parameters: Vec<u8> = u.arbitrary()?;
        let parameters = Parameters::from(parameters);

        let key = ContractKey::from((&parameters, &contract));

        Ok(Contract {
            data: contract,
            parameters,
            key,
        })
    }
}

/// Data that forms part of a contract along with the WebAssembly code.
///
/// This is supplied to the contract as a parameter to the contract's functions. Parameters are
/// typically be used to configure a contract, much like the parameters of a constructor function.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct Parameters<'a>(Cow<'a, [u8]>);

impl<'a> Parameters<'a> {
    /// Gets the number of bytes of data stored in the `Parameters`.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Extracts the owned data as a `Vec<u8>`.
    pub fn into_owned(self) -> Vec<u8> { self.0.into_owned() }
}

impl<'a> From<Vec<u8>> for Parameters<'a> {
    fn from(data: Vec<u8>) -> Self {
        Parameters(Cow::from(data))
    }
}

impl<'a> From<&'a [u8]> for Parameters<'a> {
    fn from(s: &'a [u8]) -> Self {
        Parameters(Cow::from(s))
    }
}

impl<'a> AsRef<[u8]> for Parameters<'a> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

/// Data associated with a contract that can be retrieved by Applications and Components.
///
/// For efficiency and flexibility, contract state is represented as a simple [u8] byte array.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct State<'a>(Cow<'a, [u8]>);

impl<'a> State<'a> {
    /// Gets the number of bytes of data stored in the `State`.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Extracts the owned data as a `Vec<u8>`.
    pub fn into_owned(self) -> Vec<u8> {
        self.0.into_owned()
    }

    /// Acquires a mutable reference to the owned form of the `State` data.
    pub fn to_mut(&mut self) -> &mut Vec<u8> {
        self.0.to_mut()
    }
}

impl<'a> From<Vec<u8>> for State<'a> {
    fn from(state: Vec<u8>) -> Self {
        State(Cow::from(state))
    }
}

impl<'a> From<&'a [u8]> for State<'a> {
    fn from(state: &'a [u8]) -> Self {
        State(Cow::from(state))
    }
}

impl<'a> AsRef<[u8]> for State<'a> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

impl<'a> Deref for State<'a> {
    type Target = Cow<'a, [u8]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for State<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> std::io::Read for State<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.as_ref().read(buf)
    }
}

/// Represents a modification to some state - similar to a diff in source code.
///
/// The exact format of a delta is determined by the contract. A [contract](Contract) implementation will determine whether
/// a delta is valid - perhaps by verifying it is signed by someone authorized to modify the
/// contract state. A delta may be created in response to a [State Summary](StateSummary) as part of the State
/// Synchronization mechanism.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct StateDelta<'a>(Cow<'a, [u8]>);

impl<'a> StateDelta<'a> {
    /// Gets the number of bytes of data stored in the `StateDelta`.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Extracts the owned data as a `Vec<u8>`.
    pub fn into_owned(self) -> Vec<u8> {
        self.0.into_owned()
    }
}

impl<'a> From<Vec<u8>> for StateDelta<'a> {
    fn from(delta: Vec<u8>) -> Self {
        StateDelta(Cow::from(delta))
    }
}

impl<'a> From<&'a [u8]> for StateDelta<'a> {
    fn from(delta: &'a [u8]) -> Self {
        StateDelta(Cow::from(delta))
    }
}

impl<'a> AsRef<[u8]> for StateDelta<'a> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

impl<'a> Deref for StateDelta<'a> {
    type Target = Cow<'a, [u8]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for StateDelta<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Summary of `State` changes.
///
/// Given a contract state, this is a small piece of data that can be used to determine a delta
/// between two contracts as part of the state synchronization mechanism. The format of a state
/// summary is determined by the state's contract.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StateSummary<'a>(Cow<'a, [u8]>);

impl<'a> StateSummary<'a> {
    /// Extracts the owned data as a `Vec<u8>`.
    pub fn into_owned(self) -> Vec<u8> {
        self.0.into_owned()
    }
}

impl<'a> From<Vec<u8>> for StateSummary<'a> {
    fn from(state: Vec<u8>) -> Self {
        StateSummary(Cow::from(state))
    }
}

impl<'a> From<&'a [u8]> for StateSummary<'a> {
    fn from(state: &'a [u8]) -> Self {
        StateSummary(Cow::from(state))
    }
}

impl<'a> StateSummary<'a> {
    /// Gets the number of bytes of data stored in the `StateSummary`.
    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl<'a> AsRef<[u8]> for StateSummary<'a> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

impl<'a> Deref for StateSummary<'a> {
    type Target = Cow<'a, [u8]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for StateSummary<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(any(test, feature = "testing"))]
impl<'a> arbitrary::Arbitrary<'a> for StateSummary<'static> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let data: Vec<u8> = u.arbitrary()?;
        Ok(StateSummary::from(data))
    }
}

/// The executable contract.
///
/// It is the part of the executable belonging to the full specification
/// and does not include any other metadata (like the parameters).
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractCode<'a> {
    data: Cow<'a, [u8]>,
    #[serde_as(as = "[_; CONTRACT_KEY_SIZE]")]
    key: [u8; CONTRACT_KEY_SIZE],
}

impl ContractCode<'_> {
    /// Contract key hash.
    pub fn hash(&self) -> &[u8; CONTRACT_KEY_SIZE] {
        &self.key
    }

    /// Returns the `Base58` string representation of the contract key.
    pub fn hash_str(&self) -> String {
        Self::encode_hash(&self.key)
    }

    /// Reference to contract code.
    pub fn data(&self) -> &[u8] {
        &*self.data
    }

    /// Extracts the owned contract code data as a `Vec<u8>`.
    pub fn into_data(self) -> Vec<u8> {
        self.data.to_owned().to_vec()
    }

    /// Returns the `Base58` string representation of a hash.
    pub fn encode_hash(hash: &[u8; CONTRACT_KEY_SIZE]) -> String {
        bs58::encode(hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    fn gen_key(data: &[u8]) -> [u8; CONTRACT_KEY_SIZE] {
        let mut hasher = Blake2s256::new();
        hasher.update(&data);
        let key_arr = hasher.finalize();
        debug_assert_eq!(key_arr[..].len(), CONTRACT_KEY_SIZE);
        let mut key = [0; CONTRACT_KEY_SIZE];
        key.copy_from_slice(&key_arr);
        key
    }
}

impl From<Vec<u8>> for ContractCode<'static> {
    fn from(data: Vec<u8>) -> Self {
        let key = ContractCode::gen_key(&data);
        ContractCode {
            data: Cow::from(data),
            key,
        }
    }
}

impl<'a> From<&'a [u8]> for ContractCode<'a> {
    fn from(data: &'a [u8]) -> ContractCode {
        let key = ContractCode::gen_key(data);
        ContractCode {
            data: Cow::from(data),
            key,
        }
    }
}

impl PartialEq for ContractCode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for ContractCode<'_> {}

#[cfg(any(test, feature = "testing"))]
impl<'a> arbitrary::Arbitrary<'a> for ContractCode<'static> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let data: Vec<u8> = u.arbitrary()?;
        Ok(ContractCode::from(data))
    }
}

impl std::fmt::Display for ContractCode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contract( key: ")?;
        internal_fmt_key(&self.key, f)?;
        let data: String = if self.data.len() > 8 {
            self.data[..4]
                .iter()
                .map(|b| char::from(*b))
                .chain("...".chars())
                .chain(self.data[4..].iter().map(|b| char::from(*b)))
                .collect()
        } else {
            self.data.iter().copied().map(char::from).collect()
        };
        write!(f, ", data: [{}])", data)
    }
}

/// The key representing the tuple of a `contract code` and a set of `parameters`.
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "testing"), derive(arbitrary::Arbitrary))]
#[repr(transparent)]
pub struct ContractId(#[serde_as(as = "[_; CONTRACT_KEY_SIZE]")] [u8; CONTRACT_KEY_SIZE]);

impl ContractId {
    /// `Base58` string representation of the `contract id`.
    pub fn encode(&self) -> String {
        bs58::encode(self.0)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    /// Build `ContractId` from the binary representation.
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, bs58::decode::Error> {
        let mut spec = [0; CONTRACT_KEY_SIZE];
        bs58::decode(bytes)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into(&mut spec)?;
        Ok(Self(spec))
    }
}

impl FromStr for ContractId {
    type Err = bs58::decode::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ContractId::from_bytes(s)
    }
}

impl TryFrom<String> for ContractId {
    type Error = bs58::decode::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        ContractId::from_bytes(s)
    }
}

impl<'a, T, U> From<(T, U)> for ContractId
where
    T: Borrow<Parameters<'a>>,
    U: Borrow<ContractCode<'a>>,
{
    fn from(val: (T, U)) -> Self {
        let (parameters, code_data) = (val.0.borrow(), val.1.borrow());
        generate_id(parameters, code_data)
    }
}

impl Display for ContractId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
    }
}

/// A complete key specification, that represents a cryptographic hash that identifies the contract.
#[serde_as]
#[derive(Debug, Eq, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "testing"), derive(arbitrary::Arbitrary))]
pub struct ContractKey {
    id: ContractId,
    #[serde_as(as = "Option<[_; CONTRACT_KEY_SIZE]>")]
    contract: Option<[u8; CONTRACT_KEY_SIZE]>,
}

impl PartialEq for ContractKey {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl std::hash::Hash for ContractKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.0.hash(state);
    }
}

impl From<ContractId> for ContractKey {
    fn from(id: ContractId) -> Self {
        Self { id, contract: None }
    }
}

impl<'a, T, U> From<(T, U)> for ContractKey
where
    T: Borrow<Parameters<'a>>,
    U: Borrow<ContractCode<'a>>,
{
    fn from(val: (T, U)) -> Self {
        let (parameters, code_data) = (val.0.borrow(), val.1.borrow());
        let id = generate_id(parameters, code_data);
        let contract_hash = code_data.hash();
        Self {
            id,
            contract: Some(*contract_hash),
        }
    }
}

impl ContractKey {
    /// Builds a partial `ContractKey`, the contract code part is unspecified.
    pub fn from_id(id: impl Into<String>) -> Result<Self, bs58::decode::Error> {
        let id = ContractId::try_from(id.into())?;
        Ok(Self { id, contract: None })
    }

    /// Gets the whole spec key hash.
    pub fn bytes(&self) -> &[u8] {
        self.id.0.as_ref()
    }

    /// Returns the hash of the contract code only, if the key is fully specified.
    pub fn code_hash(&self) -> Option<&[u8; CONTRACT_KEY_SIZE]> {
        self.contract.as_ref()
    }

    /// Returns the encoded hash of the contract code, if the key is fully specified.
    pub fn encoded_code_hash(&self) -> Option<String> {
        self.contract.as_ref().map(|c| {
            bs58::encode(c)
                .with_alphabet(bs58::Alphabet::BITCOIN)
                .into_string()
        })
    }

    /// Returns the decoded contract key from encoded hash of the contract key and the given
    /// parameters.
    pub fn decode(
        contract_key: impl Into<String>,
        parameters: Parameters,
    ) -> Result<Self, bs58::decode::Error> {
        let mut contract = [0; CONTRACT_KEY_SIZE];
        bs58::decode(contract_key.into())
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into(&mut contract)?;

        let mut hasher = Blake2s256::new();
        hasher.update(&contract);
        hasher.update(parameters.as_ref());
        let full_key_arr = hasher.finalize();

        let mut spec = [0; CONTRACT_KEY_SIZE];
        spec.copy_from_slice(&full_key_arr);
        Ok(Self {
            id: ContractId(spec),
            contract: Some(contract),
        })
    }

    pub fn encoded_contract_id(&self) -> String {
        self.id.encode()
    }
}

impl Deref for ContractKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.id.0
    }
}

impl std::fmt::Display for ContractKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.id.fmt(f)
    }
}

fn generate_id<'a>(parameters: &Parameters<'a>, code_data: &ContractCode<'a>) -> ContractId {
    let contract_hash = code_data.hash();

    let mut hasher = Blake2s256::new();
    hasher.update(contract_hash);
    hasher.update(parameters.as_ref());
    let full_key_arr = hasher.finalize();

    debug_assert_eq!(full_key_arr[..].len(), CONTRACT_KEY_SIZE);
    let mut spec = [0; CONTRACT_KEY_SIZE];
    spec.copy_from_slice(&full_key_arr);
    ContractId(spec)
}

#[inline]
fn internal_fmt_key(
    key: &[u8; CONTRACT_KEY_SIZE],
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let r = bs58::encode(key)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_string();
    write!(f, "{}", &r[..8])
}

#[cfg(test)]
mod test {
    use super::*;
    use once_cell::sync::Lazy;
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    static RND_BYTES: Lazy<[u8; 1024]> = Lazy::new(|| {
        let mut bytes = [0; 1024];
        let mut rng = SmallRng::from_entropy();
        rng.fill(&mut bytes);
        bytes
    });

    #[test]
    fn key_encoding() -> Result<(), Box<dyn std::error::Error>> {
        let code = ContractCode::from(vec![1, 2, 3]);
        let expected = ContractKey::from((Parameters::from(vec![]), &code));
        // let encoded_key = expected.encode();
        // println!("encoded key: {encoded_key}");
        // let encoded_code = expected.contract_part_as_str();
        // println!("encoded key: {encoded_code}");

        let decoded = ContractKey::decode(code.hash_str(), [].as_ref().into())?;
        assert_eq!(expected, decoded);
        assert_eq!(expected.code_hash(), decoded.code_hash());
        Ok(())
    }

    #[test]
    fn key_ser() -> Result<(), Box<dyn std::error::Error>> {
        let mut gen = arbitrary::Unstructured::new(&*RND_BYTES);
        let expected: ContractKey = gen.arbitrary()?;
        let encoded = bs58::encode(expected.bytes()).into_string();
        // println!("encoded key: {encoded}");

        let serialized = bincode::serialize(&expected)?;
        let deserialized: ContractKey = bincode::deserialize(&serialized)?;
        let decoded = bs58::encode(deserialized.bytes()).into_string();
        assert_eq!(encoded, decoded);
        assert_eq!(deserialized, expected);
        Ok(())
    }

    #[test]
    fn contract_ser() -> Result<(), Box<dyn std::error::Error>> {
        let mut gen = arbitrary::Unstructured::new(&*RND_BYTES);
        let expected: Contract = gen.arbitrary()?;

        let serialized = bincode::serialize(&expected)?;
        let deserialized: Contract = bincode::deserialize(&serialized)?;
        assert_eq!(deserialized, expected);
        Ok(())
    }
}
