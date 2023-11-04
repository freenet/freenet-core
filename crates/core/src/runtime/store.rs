use arrayvec::ArrayVec;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, DelegateKey};
use notify::Watcher;
use std::io::{BufReader, BufWriter, Seek, Write};
use std::path::Path;
use std::{fs::File, io::Read};

use super::RuntimeResult;
use crate::DynError;

const INTERNAL_KEY: usize = 32;
const TOMBSTONE_MARKER: usize = 1;

pub(super) enum StoreKey {
    ContractKey([u8; INTERNAL_KEY]),
    DelegateKey {
        key: [u8; INTERNAL_KEY],
        code_hash: [u8; INTERNAL_KEY],
    },
}

impl From<DelegateKey> for StoreKey {
    fn from(value: DelegateKey) -> Self {
        Self::DelegateKey {
            key: *value,
            code_hash: **value.code_hash(),
        }
    }
}

impl From<StoreKey> for DelegateKey {
    fn from(value: StoreKey) -> Self {
        let StoreKey::DelegateKey { key, code_hash } = value else {
            unreachable!()
        };
        DelegateKey::new(key, CodeHash::new(code_hash))
    }
}

impl From<ContractInstanceId> for StoreKey {
    fn from(value: ContractInstanceId) -> Self {
        Self::ContractKey(*value)
    }
}

impl From<StoreKey> for ContractInstanceId {
    fn from(value: StoreKey) -> Self {
        let StoreKey::ContractKey(value) = value else {
            unreachable!()
        };
        ContractInstanceId::new(value)
    }
}

#[repr(u8)]
enum KeyType {
    Contract = 0,
    Delegate = 1,
}

pub(super) trait StoreFsManagement {
    type MemContainer: Send + Sync + 'static;
    type Key: Clone + From<StoreKey>;
    type Value: AsRef<[u8]> + for<'x> TryFrom<&'x [u8], Error = std::io::Error>;

    fn insert_in_container(
        container: &mut Self::MemContainer,
        key_and_offset: (Self::Key, u64),
        value: Self::Value,
    );

    fn watch_changes(
        mut container: Self::MemContainer,
        key_file_path: &Path,
    ) -> Result<(), DynError> {
        let key_path = key_file_path.to_path_buf();
        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(ev) => {
                    if let notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) = ev.kind {
                        if let Err(err) = Self::load_from_file(key_path.as_path(), &mut container) {
                            tracing::error!("{err}")
                        }
                    }
                }
                Err(err) => tracing::error!("{err}"),
            },
        )?;
        watcher.watch(key_file_path, notify::RecursiveMode::NonRecursive)?;
        Ok(())
    }

    fn insert(
        file: &mut BufWriter<File>,
        mem_container: &mut Self::MemContainer,
        key: Self::Key,
        value: Self::Value,
    ) -> RuntimeResult<()>
    where
        StoreKey: From<Self::Key>,
    {
        // The full key is the tombstone marker byte + kind + [internal key content]  + size of value
        let internal_key: StoreKey = key.clone().into();
        file.write_u8(false as u8)?;
        match internal_key {
            StoreKey::ContractKey(key) => {
                file.write_u8(KeyType::Contract as u8)?;
                file.write_all(&key)?;
            }
            StoreKey::DelegateKey { key, code_hash } => {
                file.write_u8(KeyType::Delegate as u8)?;
                file.write_all(&key)?;
                file.write_all(&code_hash)?;
            }
        }
        file.write_u32::<BigEndian>(value.as_ref().len() as u32)?;
        file.write_all(value.as_ref())?;
        file.flush()?;
        Self::insert_in_container(mem_container, (key, file.stream_position()?), value);
        Ok(())
    }

    fn remove(key_file_path: &Path, key_offset: u64) -> RuntimeResult<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(key_file_path)?;
        file.seek(std::io::SeekFrom::Start(key_offset))?;
        // Mark tombstone byte as true
        file.write_u8(true as u8)?;
        Ok(())
    }

    fn load_from_file(
        key_file_path: &Path,
        container: &mut Self::MemContainer,
    ) -> RuntimeResult<()> {
        let mut file = BufReader::new(File::open(key_file_path)?);
        let mut first_key_part =
            [0u8; TOMBSTONE_MARKER + std::mem::size_of::<KeyType>() + INTERNAL_KEY];
        let mut key_cursor = 0;
        while file.read_exact(&mut first_key_part).is_ok() {
            let deleted = first_key_part[0] != 0;
            let key_type = match first_key_part[1] {
                0 => KeyType::Contract,
                1 => KeyType::Delegate,
                _ => unreachable!(),
            };
            if !deleted {
                let store_key = match key_type {
                    KeyType::Contract => {
                        let mut contract_key = [0; INTERNAL_KEY];
                        contract_key.copy_from_slice(&first_key_part[2..2 + INTERNAL_KEY]);
                        StoreKey::ContractKey(contract_key)
                    }
                    KeyType::Delegate => {
                        let mut delegate_key = [0; INTERNAL_KEY];
                        let mut code_hash = [0; INTERNAL_KEY];
                        delegate_key.copy_from_slice(&first_key_part[2..2 + INTERNAL_KEY]);
                        file.read_exact(&mut code_hash)?;
                        StoreKey::DelegateKey {
                            key: delegate_key,
                            code_hash,
                        }
                    }
                };
                let value_len = file.read_u32::<BigEndian>()?;
                let value = {
                    if value_len == 32 {
                        let buf = &mut ArrayVec::<u8, 32>::from([0; 32]);
                        file.read_exact(&mut *buf)?;
                        Self::Value::try_from(&*buf)?
                    } else {
                        let mut buf = vec![0; value_len as usize];
                        file.read_exact(&mut buf)?;
                        Self::Value::try_from(&buf)?
                    }
                };
                Self::insert_in_container(container, (store_key.into(), key_cursor), value);
            } else {
                let skip = match key_type {
                    KeyType::Contract => file.read_u32::<BigEndian>()?,
                    KeyType::Delegate => {
                        // skip the code hash part
                        file.seek_relative(32)?;
                        file.read_u32::<BigEndian>()?
                    }
                };
                file.seek_relative(skip as i64)?;
            }
            key_cursor = file.stream_position()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, sync::Arc};

    use super::*;
    use dashmap::DashMap;
    use tempfile::TempDir;

    struct TestStore1;

    impl StoreFsManagement for TestStore1 {
        type MemContainer = Arc<DashMap<ContractInstanceId, (u64, CodeHash)>>;
        type Key = ContractInstanceId;
        type Value = CodeHash;

        fn insert_in_container(
            container: &mut Self::MemContainer,
            (key, offset): (Self::Key, u64),
            value: Self::Value,
        ) {
            container.insert(key, (offset, value));
        }
    }

    struct TestStore2;

    impl StoreFsManagement for TestStore2 {
        type MemContainer = Arc<DashMap<DelegateKey, (u64, CodeHash)>>;
        type Key = DelegateKey;
        type Value = CodeHash;

        fn insert_in_container(
            container: &mut Self::MemContainer,
            (key, offset): (Self::Key, u64),
            value: Self::Value,
        ) {
            container.insert(key, (offset, value));
        }
    }

    #[test]
    fn test_store() {
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let contract_keys_file_path = temp_dir.path().join("contract_keys");
        let delegate_keys_file_path = temp_dir.path().join("delegate_keys");

        let key_1 = ContractInstanceId::new([1; 32]);
        let expected_value_1 = CodeHash::new([2; 32]);
        let key_2 = DelegateKey::new([3; 32], CodeHash::new([4; 32]));
        let expected_value_2 = CodeHash::new([5; 32]);

        // Test the update function
        {
            let mut file_1 = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(&contract_keys_file_path)
                    .expect("Failed to open key file"),
            );
            let mut file_2 = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(&delegate_keys_file_path)
                    .expect("Failed to open key file"),
            );
            let mut container_1 = <TestStore1 as StoreFsManagement>::MemContainer::default();
            let mut container_2 = <TestStore2 as StoreFsManagement>::MemContainer::default();

            TestStore1::insert(&mut file_1, &mut container_1, key_1, expected_value_1)
                .expect("Failed to update");

            TestStore2::insert(
                &mut file_2,
                &mut container_2,
                key_2.clone(),
                expected_value_2,
            )
            .expect("Failed to update");
        }

        // Test the load_from_file function
        {
            let mut new_container_1 = <TestStore1 as StoreFsManagement>::MemContainer::default();
            TestStore1::load_from_file(&contract_keys_file_path, &mut new_container_1)
                .expect("Failed to load from file");

            let mut new_container_2 = <TestStore2 as StoreFsManagement>::MemContainer::default();
            TestStore2::load_from_file(&delegate_keys_file_path, &mut new_container_2)
                .expect("Failed to load from file");

            // Check if the container has the updated key-value pair
            let loaded_value_1 = new_container_1.get(&key_1).expect("Key not found");
            let loaded_value_2 = new_container_2.get(&key_2).expect("Key not found");

            assert_eq!(expected_value_1, loaded_value_1.value().1);
            assert_eq!(expected_value_2, loaded_value_2.value().1);
        }

        // Test the remove function for TestStore1
        {
            let key_file_path = &contract_keys_file_path;
            let key_offset = 0;

            TestStore1::remove(key_file_path, key_offset).expect("Failed to remove key");

            // Reload the container from the key file and check if the key is removed
            let mut new_container_1 = <TestStore1 as StoreFsManagement>::MemContainer::default();
            TestStore1::load_from_file(key_file_path, &mut new_container_1)
                .expect("Failed to load from file");

            let loaded_value_1 = new_container_1.get(&key_1);
            assert!(loaded_value_1.is_none(), "Key still exists");
        }
    }
}
