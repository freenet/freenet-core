use std::{ops::Deref, path::PathBuf, sync::Arc};

use blake2::{Blake2b512, Digest};
use serde::{Deserialize, Deserializer, Serialize};

use crate::ring::Location;

mod handler;
mod runtime;
mod store;
mod test;

#[cfg(test)]
pub(crate) use handler::test::TestContractHandler;
pub(crate) use handler::{
    contract_handler_channel, CHSenderHalve, ContractHandler, ContractHandlerChannel,
    ContractHandlerEvent, SQLiteContractHandler, SqlDbError, StoreResponse,
};
pub(crate) use store::ContractStoreError;
pub(crate) use test::MockRuntime;
#[cfg(test)]
pub(crate) use test::{MemoryContractHandler, SimStoreError};

const CONTRACT_KEY_SIZE: usize = 64;

pub(crate) async fn contract_handling<CH, Err>(
    mut contract_handler: CH,
) -> Result<(), ContractError<Err>>
where
    CH: ContractHandler<Error = Err> + Send + 'static,
    Err: std::error::Error + Send + 'static,
{
    loop {
        let res = contract_handler.channel().recv_from_listener().await?;
        match res {
            (
                id,
                ContractHandlerEvent::FetchQuery {
                    key,
                    fetch_contract,
                },
            ) => {
                let contract = if fetch_contract {
                    contract_handler
                        .contract_store()
                        .fetch_contract(&key)
                        .await?
                } else {
                    None
                };

                let response = contract_handler
                    .get_value(&key)
                    .await
                    .map(|value| StoreResponse { value, contract });

                contract_handler
                    .channel()
                    .send_to_listener(id, ContractHandlerEvent::FetchResponse { key, response })
                    .await?;
            }
            (id, ContractHandlerEvent::Cache(contract)) => {
                match contract_handler
                    .contract_store()
                    .store_contract(contract)
                    .await
                {
                    Ok(_) => {
                        contract_handler
                            .channel()
                            .send_to_listener(id, ContractHandlerEvent::CacheResult(Ok(())))
                            .await?;
                    }
                    Err(err) => {
                        contract_handler
                            .channel()
                            .send_to_listener(id, ContractHandlerEvent::CacheResult(Err(err)))
                            .await?;
                    }
                }
            }
            (id, ContractHandlerEvent::PushQuery { key, value }) => {
                let put_result = contract_handler.put_value(&key, value).await;
                contract_handler
                    .channel()
                    .send_to_listener(
                        id,
                        ContractHandlerEvent::PushResponse {
                            new_value: put_result,
                        },
                    )
                    .await?;
            }
            _ => unreachable!(),
        }
    }
}

/// Main abstraction for representing a contract in binary form.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    data: Arc<Vec<u8>>,
    #[serde(serialize_with = "<[_]>::serialize")]
    #[serde(deserialize_with = "contract_key_deser")]
    key: [u8; CONTRACT_KEY_SIZE],
}

impl Contract {
    pub fn new(data: Vec<u8>) -> Self {
        let mut hasher = Blake2b512::new();
        hasher.update(&data);
        let key_arr = hasher.finalize();
        debug_assert_eq!((&key_arr[..]).len(), CONTRACT_KEY_SIZE);
        let mut key = [0; CONTRACT_KEY_SIZE];
        key.copy_from_slice(&key_arr);

        Self {
            data: Arc::new(data),
            key,
        }
    }

    pub fn key(&self) -> ContractKey {
        ContractKey(self.key)
    }
}

impl TryInto<Vec<u8>> for Contract {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Arc::try_unwrap(self.data).map_err(|_| anyhow::anyhow!("non-unique contract ref"))
    }
}

impl PartialEq for Contract {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Contract {}

/// The key representing a contract.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, Hash)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct ContractKey(
    #[serde(deserialize_with = "contract_key_deser")]
    #[serde(serialize_with = "<[_]>::serialize")]
    [u8; 64],
);

impl ContractKey {
    pub(crate) fn bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// The corresponding location to this contract
    #[inline]
    pub fn location(&self) -> Location {
        Location::from(self)
    }
}

impl From<ContractKey> for PathBuf {
    fn from(val: ContractKey) -> Self {
        let r = hex::encode(val.0);
        PathBuf::from(r)
    }
}

impl std::fmt::Display for ContractKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = hex::encode(self.0);
        write!(f, "{}", &r[..8])
    }
}

impl<'a> arbitrary::Arbitrary<'a> for Contract {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let data: Vec<u8> = u.arbitrary()?;
        Ok(Contract::new(data))
    }
}

// A bit wasteful but cannot deserialize directly into [u8; 64]
// with current version of serde
fn contract_key_deser<'de, D>(deserializer: D) -> Result<[u8; 64], D::Error>
where
    D: Deserializer<'de>,
{
    let data: Vec<u8> = Deserialize::deserialize(deserializer)?;
    let mut key = [0u8; CONTRACT_KEY_SIZE];
    key.copy_from_slice(&data);
    Ok(key)
}

/// The value for a contract.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct ContractValue(Arc<Vec<u8>>);

impl ContractValue {
    pub fn new(bytes: Vec<u8>) -> Self {
        ContractValue(Arc::new(bytes))
    }
}

impl Deref for ContractValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ContractError<CErr> {
    #[error("failed while storing a contract")]
    StorageError(CErr),
    #[error("contract {0} not found in storage")]
    ContractNotFound(ContractKey),
    #[error("handler channel dropped")]
    ChannelDropped(Box<ContractHandlerEvent<CErr>>),
    #[error("no response received from handler")]
    NoEvHandlerResponse,
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}
