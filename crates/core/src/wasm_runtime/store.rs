use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use either::Either;
use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, DelegateKey};
use notify::Watcher;
use std::fs::{self, OpenOptions};
use std::io::{self, BufReader, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{fs::File, io::Read};

use crate::DynError;

const INTERNAL_KEY: usize = 32;
const TOMBSTONE_MARKER: usize = 1;

pub(super) struct SafeWriter<S> {
    file: BufWriter<File>,
    lock_file_path: PathBuf,
    compact: bool,
    _marker: std::marker::PhantomData<fn(S) -> S>,
}

impl<S: StoreFsManagement> SafeWriter<S> {
    pub fn new(path: &Path, compact: bool) -> Result<Self, io::Error> {
        let file = if compact {
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path)?
        } else {
            OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(path)?
        };
        let s = Self {
            file: BufWriter::new(file),
            compact,
            lock_file_path: path.with_extension("lock"),
            _marker: std::marker::PhantomData,
        };
        Ok(s)
    }

    /// Inserts a new record and returns the offset
    fn insert_record(&mut self, key: StoreKey, value: &[u8]) -> std::io::Result<u64> {
        self.check_lock();
        // The full key is the tombstone marker byte + kind + [internal key content]  + size of value
        self.file.write_u8(false as u8)?;
        let mut traversed = 1;
        match key {
            StoreKey::ContractKey(key) => {
                self.file.write_u8(KeyType::Contract as u8)?;
                self.file.write_all(&key)?;
            }
            StoreKey::DelegateKey { key, code_hash } => {
                self.file.write_u8(KeyType::Delegate as u8)?;
                self.file.write_all(&key)?;
                self.file.write_all(&code_hash)?;
                traversed += 32; // additional code_hash bytes
            }
        }
        traversed += 1 + 32; // key + type marker
        self.file.write_u32::<BigEndian>(value.len() as u32)?;
        traversed += std::mem::size_of::<u32>();
        self.file.write_all(value)?;
        traversed += value.len();
        self.file.flush()?;
        let current_offset = self.file.stream_position()?;
        Ok(current_offset - traversed as u64)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.check_lock();
        self.file.flush()
    }
}

impl<S> SafeWriter<S> {
    fn check_lock(&self) {
        while !self.compact && self.lock_file_path.exists() {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl<S> Drop for SafeWriter<S> {
    fn drop(&mut self) {
        self.check_lock();
    }
}

#[derive(Debug)]
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

pub(super) trait StoreFsManagement: Sized {
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
        let key_path_cp = key_path.clone();
        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(ev) => {
                    if let notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) = ev.kind {
                        if let Err(err) =
                            Self::load_from_file(key_path_cp.as_path(), &mut container)
                        {
                            tracing::error!("{err}")
                        }
                    }
                }
                Err(err) => tracing::error!("{err}"),
            },
        )?;
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(5 * 60));
            if let Err(err) = compact_index_file::<Self>(&key_path) {
                tracing::warn!("Failed index file ({key_path:?}) compaction: {err}");
            }
        });
        watcher.watch(key_file_path, notify::RecursiveMode::NonRecursive)?;
        Ok(())
    }

    /// Insert in index file and returns the offset at which this record resides.
    fn insert(
        file: &mut SafeWriter<Self>,
        key: Self::Key,
        value: &Self::Value,
    ) -> std::io::Result<u64>
    where
        StoreKey: From<Self::Key>,
    {
        // The full key is the tombstone marker byte + kind + [internal key content]  + size of value
        let internal_key: StoreKey = key.into();
        let offset = file.insert_record(internal_key, value.as_ref())?;
        Ok(offset)
    }

    fn remove(key_file_path: &Path, key_offset: u64) -> std::io::Result<()> {
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
    ) -> std::io::Result<()> {
        let mut file = BufReader::new(File::open(key_file_path)?);
        let mut key_cursor = 0;
        while let Ok(rec) = process_record(&mut file) {
            if let Some((store_key, value)) = rec {
                let store_key = store_key.into();
                let value = match value {
                    Either::Left(v) => Self::Value::try_from(&v),
                    Either::Right(v) => Self::Value::try_from(&v),
                }?;
                Self::insert_in_container(container, (store_key, key_cursor), value);
            }
            key_cursor = file.stream_position()?;
        }
        Ok(())
    }
}

#[allow(clippy::type_complexity)]
fn process_record<T>(
    reader: &mut BufReader<T>,
) -> std::io::Result<Option<(StoreKey, Either<[u8; 32], Vec<u8>>)>>
where
    T: Read + Seek,
{
    let mut key_part = [0u8; TOMBSTONE_MARKER + std::mem::size_of::<KeyType>()];
    reader.read_exact(&mut key_part)?;

    let deleted = key_part[0] != 0;
    let key_type = match key_part[1] {
        0 => KeyType::Contract,
        1 => KeyType::Delegate,
        _ => unreachable!(),
    };

    if !deleted {
        let store_key = match key_type {
            KeyType::Contract => {
                let mut contract_key = [0; INTERNAL_KEY];
                reader.read_exact(&mut contract_key)?;
                StoreKey::ContractKey(contract_key)
            }
            KeyType::Delegate => {
                let mut delegate_key = [0; INTERNAL_KEY];
                let mut code_hash = [0; INTERNAL_KEY];
                reader.read_exact(&mut delegate_key)?;
                reader.read_exact(&mut code_hash)?;
                StoreKey::DelegateKey {
                    key: delegate_key,
                    code_hash,
                }
            }
        };

        // Write the value part
        let value_len = reader.read_u32::<BigEndian>()?;
        let value = if value_len == 32 {
            let mut value = [0u8; 32];
            reader.read_exact(&mut value)?;
            Either::Left(value)
        } else {
            let mut value = vec![0u8; value_len as usize];
            reader.read_exact(&mut value)?;
            Either::Right(value)
        };
        Ok(Some((store_key, value)))
    } else {
        // Skip the record if deleted
        let value_len = match key_type {
            KeyType::Contract => {
                reader.seek_relative(32)?; // skip the actual key
                reader.read_u32::<BigEndian>()? // get the value len
            }
            KeyType::Delegate => {
                reader.seek_relative(32)?; // skip the delegate key
                reader.seek_relative(32)?; // skip the code hash
                reader.read_u32::<BigEndian>()? // get the value len
            }
        };
        reader.seek_relative(value_len as i64)?;
        Ok(None)
    }
}

fn compact_index_file<S: StoreFsManagement>(key_file_path: &Path) -> std::io::Result<()> {
    // Define the path to the lock file
    let lock_file_path = key_file_path.with_extension("lock");

    // Attempt to create the lock file
    let mut opts = fs::OpenOptions::new();
    opts.create_new(true).write(true);
    match opts.open(&lock_file_path) {
        Ok(_) => {
            // The lock file was created successfully, so a compaction can proceed
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // The lock file already exists, so a compaction is in progress
            return Ok(());
        }
        Err(e) => {
            // An unexpected error occurred
            return Err(e);
        }
    }

    let original_file = OpenOptions::new()
        .truncate(false)
        .read(true)
        .open(key_file_path)?;

    // Create a new temporary file to write compacted data
    let temp_file_path = key_file_path.with_extension("tmp");

    // Read the original file and compact data into the temp file
    let mut original_reader = BufReader::new(original_file);
    let mut temp_writer = SafeWriter::<S>::new(&temp_file_path, true).map_err(|e| {
        if let Err(e) = fs::remove_file(&lock_file_path) {
            eprintln!("{}:{}: Failed to remove lock file: {e}", file!(), line!());
        }
        e
    })?;

    let mut any_deleted = false; // Track if any deleted records were found

    loop {
        match process_record(&mut original_reader) {
            Ok(Some((store_key, value))) => {
                let value = match &value {
                    Either::Left(v) => v.as_slice(),
                    Either::Right(v) => v.as_slice(),
                };
                if let Err(err) = temp_writer.insert_record(store_key, value) {
                    if let Err(e) = fs::remove_file(&lock_file_path) {
                        eprintln!("{}:{}: Failed to remove lock file: {e}", file!(), line!());
                    }
                    return Err(err);
                }
            }
            Ok(None) => {
                // Skip record
                any_deleted = true; // A deleted record was found
            }
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Done
                break;
            }
            Err(other) => {
                // Handle other errors gracefully
                if let Err(e) = fs::remove_file(&lock_file_path) {
                    eprintln!("{}:{}: Failed to remove lock file: {e}", file!(), line!());
                }
                return Err(other);
            }
        }
    }

    // Check if any deleted records were found; if not, skip compaction
    if !any_deleted {
        if let Err(e) = fs::remove_file(&lock_file_path) {
            eprintln!("{}:{}: Failed to remove lock file: {e}", file!(), line!());
        }
        return Ok(());
    }

    // Clean up and finalize the compaction process
    if let Err(e) = temp_writer.flush() {
        if let Err(e) = fs::remove_file(&lock_file_path) {
            eprintln!("{}:{}: Failed to remove lock file: {e}", file!(), line!());
        }
        return Err(e);
    }

    // Replace the original file with the temporary file
    if let Err(e) = fs::rename(&temp_file_path, key_file_path) {
        if let Err(e) = fs::remove_file(&lock_file_path) {
            eprintln!("{}:{}: Failed to remove lock file: {e}", file!(), line!());
        }
        return Err(e);
    }

    // Remove the lock file
    fs::remove_file(&lock_file_path).map_err(|e| {
        eprintln!("{}:{}: Failed to remove lock file: {e}", file!(), line!());
        e
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use crate::util::tests::get_temp_dir;

    use super::*;
    use dashmap::DashMap;

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
        let temp_dir = get_temp_dir();
        let contract_keys_file_path = temp_dir.path().join("contract_keys");
        let delegate_keys_file_path = temp_dir.path().join("delegate_keys");

        let key_1 = ContractInstanceId::new([1; 32]);
        let expected_value_1 = CodeHash::new([2; 32]);
        let key_2 = DelegateKey::new([3; 32], CodeHash::new([4; 32]));
        let expected_value_2 = CodeHash::new([5; 32]);

        // Test the update function
        {
            let mut file_1 = SafeWriter::new(&contract_keys_file_path, false).expect("failed");
            let mut file_2 = SafeWriter::new(&delegate_keys_file_path, false).expect("failed");
            let container_1 = <TestStore1 as StoreFsManagement>::MemContainer::default();
            let container_2 = <TestStore2 as StoreFsManagement>::MemContainer::default();

            let offset = TestStore1::insert(&mut file_1, key_1, &expected_value_1)
                .expect("Failed to update");
            container_1.insert(key_1, (offset, expected_value_1));

            let offset = TestStore2::insert(&mut file_2, key_2.clone(), &expected_value_2)
                .expect("Failed to update");
            container_2.insert(key_2.clone(), (offset, expected_value_2));
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

    #[test]
    fn test_concurrent_updates() {
        const NUM_THREADS: usize = 4;

        let temp_dir = get_temp_dir();
        let contract_keys_file_path = temp_dir.path().join("contract_keys");
        std::fs::File::create(&contract_keys_file_path).expect("Failed to create file");

        let container = <TestStore1 as StoreFsManagement>::MemContainer::default();
        let barrier = Arc::new(Barrier::new(NUM_THREADS));

        let mut handles = vec![];
        for i in [0, 10, 20, 30] {
            let shared_data = container.clone();
            let barrier = barrier.clone();
            let path = &temp_dir.path().join("contract_keys");
            let mut file = SafeWriter::new(path, false).expect("failed");
            let key_file_path = contract_keys_file_path.clone();
            let handle = std::thread::spawn(move || {
                // Wait for all threads to reach this point
                barrier.wait();
                create_test_data(&mut file, &key_file_path, shared_data, i)
            });
            handles.push(handle);
        }

        // Wait for all threads to finish
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Assert the correctness of append-only updates in the shared data
        // Check if the shared data contains the expected content after updates
        let container = Arc::try_unwrap(container).unwrap();
        assert_eq!(container.len(), NUM_THREADS * 7);
        let mut new_container = <TestStore1 as StoreFsManagement>::MemContainer::default();
        TestStore1::load_from_file(&contract_keys_file_path, &mut new_container)
            .expect("Failed to load from file");
        assert_eq!(new_container.len(), container.len());
        for i in [0, 10, 20, 30] {
            for j in [0, 1, 2, 4, 5, 7, 8] {
                assert!(
                    new_container.contains_key(&ContractInstanceId::new([i + j; 32])),
                    "does not have non-deleted key: {}",
                    i + j
                );
            }
            for j in [3, 6, 9] {
                assert!(
                    !new_container.contains_key(&ContractInstanceId::new([i + j; 32])),
                    "has deleted key: {}",
                    i + j
                );
            }
        }
    }

    #[test]
    fn test_concurrent_compaction() {
        for _ in 0..100 {
            concurrent_compaction();
        }
    }

    fn concurrent_compaction() {
        let temp_dir = get_temp_dir();
        let key_file_path = temp_dir.path().join("data.dat");
        std::fs::File::create(&key_file_path).expect("Failed to create file");

        let num_threads = 4;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_threads));

        let container = <TestStore1 as StoreFsManagement>::MemContainer::default();

        // Start multiple threads to run the compaction function concurrently
        let handles: Vec<_> = [0, 10, 20, 30]
            .into_iter()
            .map(|i| {
                let key_file_path = key_file_path.clone();
                let barrier = barrier.clone();
                let shared_data = container.clone();
                let mut file = SafeWriter::new(&key_file_path, false).expect("failed");
                std::thread::spawn(move || {
                    barrier.wait();
                    // concurrently creates/removes some data and compacts
                    if [10, 30].contains(&i) {
                        create_test_data(&mut file, &key_file_path, shared_data, i);
                    } else if let Err(err) = super::compact_index_file::<TestStore1>(&key_file_path)
                    {
                        eprintln!("Thread encountered an error during compaction: {err}");
                        return Err(err);
                    }
                    barrier.wait();
                    // compact a last time so we know what data to compare against
                    super::compact_index_file::<TestStore1>(&key_file_path).map_err(|err| {
                        eprintln!("Thread encountered an error during compaction: {err}");
                        err
                    })
                })
            })
            .collect();

        // Wait for all threads to finish
        for handle in handles {
            handle
                .join()
                .expect("Thread panicked")
                .expect("Compaction not completed");
        }

        let mut file = BufReader::new(File::open(key_file_path).expect("Couldn't open file"));

        let mut deleted = 0;
        let mut keys = vec![];
        while let Ok(rec) = process_record(&mut file) {
            match rec {
                Some((key, _)) => {
                    if let StoreKey::ContractKey(key) = key {
                        keys.push(key);
                    }
                }
                None => {
                    deleted += 1;
                }
            }
        }
        assert_eq!(keys.len(), 14);
        for i in [10, 30] {
            for j in [0, 1, 2, 4, 5, 7, 8] {
                assert!(
                    keys.contains(&[i + j; 32]),
                    "does not have non-deleted key: {}",
                    i + j
                );
            }
        }
        assert_eq!(deleted, 0); // should be clean after compaction
    }

    fn create_test_data(
        file: &mut SafeWriter<TestStore1>,
        test_path: &Path,
        shared_data: <TestStore1 as StoreFsManagement>::MemContainer,
        thread: u8,
    ) {
        for j in 0..10 {
            let key = ContractInstanceId::new([thread + j as u8; 32]);
            let value = CodeHash::new([thread + j as u8; 32]);
            let offset = TestStore1::insert(file, key, &value).expect("Failed to update");
            shared_data.insert(key, (offset, value));
        }
        for j in [3, 6, 9] {
            let key = ContractInstanceId::new([thread + j as u8; 32]);
            let key_offset = shared_data.remove(&key).unwrap().1 .0;
            TestStore1::remove(test_path, key_offset).expect("Failed to remove key");
        }
    }
}
