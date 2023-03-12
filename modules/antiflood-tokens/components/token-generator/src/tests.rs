use super::*;

mod token_assignment {
    use super::*;
    use chrono::{NaiveDate, Timelike};
    use locutus_aft_interface::Tier;
    use once_cell::sync::Lazy;
    use rsa::{pkcs1v15::Signature, RsaPublicKey};

    fn get_assignment_date(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        let naive = NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        DateTime::<Utc>::from_utc(naive, Utc)
    }

    const TEST_TIER: Tier = Tier::Day1;
    const MAX_DURATION_1Y: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 3600);

    const RSA_4096_PUB_PEM: &str = include_str!("../../../interfaces/examples/rsa4096-pub.pem");
    static PK: Lazy<RsaPublicKey> = Lazy::new(|| {
        <RsaPublicKey as rsa::pkcs1::DecodeRsaPublicKey>::from_pkcs1_pem(RSA_4096_PUB_PEM).unwrap()
    });

    static ID: Lazy<ContractInstanceId> = Lazy::new(|| {
        let rnd = [1; 32];
        let mut gen = arbitrary::Unstructured::new(&rnd);
        gen.arbitrary().unwrap()
    });

    #[test]
    fn free_spot_first() {
        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![TokenAssignment {
                tier: TEST_TIER,
                time_slot: get_assignment_date(2023, 1, 25),
                assignee: PK.clone(),
                signature: Signature::from(vec![1u8; 64].into_boxed_slice()),
                assignment_hash: [0; 32],
                token_record: *ID,
            }],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
            get_assignment_date(2023, 1, 27),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 27));
    }

    #[test]
    fn free_spot_skip_first() {
        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 27),
                    assignee: PK.clone(),
                    signature: Signature::from(vec![1u8; 64].into_boxed_slice()),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2023, 1, 26),
                    assignee: PK.clone(),
                    signature: Signature::from(vec![1u8; 64].into_boxed_slice()),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
            ],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
            get_assignment_date(2023, 1, 27).with_minute(1).unwrap(),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 28));
    }

    #[test]
    fn free_spot_skip_gap_1() {
        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 27),
                    assignee: PK.clone(),
                    signature: Signature::from(vec![1u8; 64].into_boxed_slice()),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 29),
                    assignee: PK.clone(),
                    signature: Signature::from(vec![1u8; 64].into_boxed_slice()),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
            ],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
            get_assignment_date(2023, 1, 27),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 28));

        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 27),
                    assignee: PK.clone(),
                    signature: Signature::from(vec![1u8; 64].into_boxed_slice()),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 28),
                    assignee: PK.clone(),
                    signature: Signature::from(vec![1u8; 64].into_boxed_slice()),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 30),
                    assignee: PK.clone(),
                    signature: Signature::from(vec![1u8; 64].into_boxed_slice()),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
            ],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
            get_assignment_date(2023, 1, 27).with_minute(1).unwrap(),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 29));
    }

    #[test]
    fn free_spot_new() {
        let records = TokenAllocationRecord::new(HashMap::new());
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
            get_assignment_date(2023, 1, 27).with_minute(1).unwrap(),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 28));
    }
}

mod integration_test {
    use chacha20poly1305::{
        aead::{AeadCore, KeyInit, OsRng},
        XChaCha20Poly1305,
    };
    use locutus_aft_interface::{AllocationCriteria, Tier, TokenAllocationRecord};
    use locutus_runtime::{
        ApplicationMessage, ComponentRuntimeInterface, ComponentStore, ContractContainer,
        ContractInstanceId, ContractKey, ContractStore, InboundComponentMsg, Runtime, SecretsId,
        SecretsStore, WasmAPIVersion,
    };
    use locutus_stdlib::prelude::{Component, ContractCode, WrappedContract};
    use rand_chacha::rand_core::SeedableRng;
    use rsa::RsaPrivateKey;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use crate::{RequestNewToken, TokenComponentMessage};

    static TEST_NO: AtomicUsize = AtomicUsize::new(0);

    fn test_dir(prefix: &str) -> PathBuf {
        let test_dir = std::env::temp_dir().join("locutus-test").join(format!(
            "{prefix}-test-{}",
            TEST_NO.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        if !test_dir.exists() {
            std::fs::create_dir_all(&test_dir).unwrap();
        }
        test_dir
    }

    fn get_test_module(dir_name: &str, name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let module_path = {
            const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
            let contracts = PathBuf::from(CONTRACTS_DIR);
            let mut dirs = contracts.ancestors();
            let path = dirs.nth(2).unwrap();
            path.join(dir_name).join(name.replace('_', "-"))
        };
        const TARGET_DIR_VAR: &str = "CARGO_TARGET_DIR";
        let target = std::env::var(TARGET_DIR_VAR).map_err(|_| "CARGO_TARGET_DIR should be set")?;
        println!("trying to compile the test contract, target: {target}");
        // attempt to compile it
        const RUST_TARGET_ARGS: &[&str] = &["build", "--target"];
        const _WASI_TARGET: &str = "wasm32-wasi";
        const DEFAULT_TARGET: &str = "wasm32-unknown-unknown";
        let cmd_args = RUST_TARGET_ARGS
            .iter()
            .copied()
            .chain([DEFAULT_TARGET])
            .collect::<Vec<_>>();
        let mut child = Command::new("cargo")
            .args(&cmd_args)
            .current_dir(&module_path)
            .spawn()?;
        child.wait()?;
        let file_name = format!("locutus_{}", name.replace('-', "_"));
        let output_file = Path::new(&target)
            .join(DEFAULT_TARGET)
            .join("debug")
            .join(file_name)
            .with_extension("wasm");
        println!("output file: {output_file:?}");
        Ok(std::fs::read(output_file)?)
    }

    fn set_up_aft<'a>(
        key_pair: &RsaPrivateKey,
        contract_name: &str,
        component_name: &str,
    ) -> Result<(Component<'a>, SecretsId, ContractKey, Runtime), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

        // Setup contract
        let contracts_dir = test_dir("contract");
        let mut contract_store = ContractStore::new(contracts_dir, 10_000)?;
        let contract_bytes = WrappedContract::new(
            Arc::new(ContractCode::from(get_test_module(
                "contracts",
                contract_name,
            )?)),
            vec![].into(),
        );
        let contract = ContractContainer::Wasm(WasmAPIVersion::V1(contract_bytes));
        let contract_key = contract.key();
        contract_store.store_contract(contract)?;

        // Setup component
        let components_dir = test_dir("aft-component");
        let secrets_dir = test_dir("aft-secret");

        let mut component_store = ComponentStore::new(components_dir, 10_000)?;
        let mut secret_store = SecretsStore::new(secrets_dir)?;

        let component = {
            let bytes = get_test_module("components", component_name)?;
            Component::from(bytes)
        };

        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let _ = secret_store.register_component(component.key().clone(), cipher, nonce);
        let _ = component_store.store_component(component.clone());

        // Store secret token
        let secret_id = SecretsId::new(vec![0, 1, 2]);
        let encoded = bincode::serialize(&key_pair.to_public_key()).unwrap();
        secret_store
            .store_secret(&component.key().clone(), &secret_id, encoded)
            .unwrap();

        let runtime = Runtime::build(contract_store, component_store, secret_store, false).unwrap();

        //runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
        Ok((component, secret_id, contract_key, runtime))
    }

    #[test]
    fn test_process_allocated_token() {
        let mut csprng = rand_chacha::ChaChaRng::seed_from_u64(1);
        let private_key = RsaPrivateKey::new(&mut csprng, 8).unwrap();

        let (component, secret_id, contract_key, mut runtime) =
            set_up_aft(&private_key, "token-allocation-record", "token-generator").unwrap();
        let app = ContractInstanceId::try_from(contract_key.to_string()).unwrap();
        let criteria = AllocationCriteria::new(
            Tier::Day1,
            std::time::Duration::from_secs(365 * 24 * 3600),
            app,
        )
        .unwrap();

        let request_new_token = RequestNewToken {
            request_id: 1,
            component_id: secret_id,
            criteria,
            records: TokenAllocationRecord::new(std::collections::HashMap::new()),
            assignee: private_key.to_public_key(),
            assignment_hash: [0; 32],
        };

        let message = TokenComponentMessage::RequestNewToken(request_new_token);
        let payload: Vec<u8> = bincode::serialize(&message).unwrap();

        let inbound_message =
            InboundComponentMsg::ApplicationMessage(ApplicationMessage::new(app, payload, false));
        let _outbound = runtime
            .inbound_app_message(component.key(), vec![inbound_message])
            .unwrap();
    }
}
