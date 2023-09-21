use chacha20poly1305::{
    aead::{AeadCore, KeyInit, OsRng},
    XChaCha20Poly1305,
};
use rand_chacha::rand_core::SeedableRng;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use freenet::runtime::{
    ContractStore,
    DelegateRuntimeInterface,
    DelegateStore,
    // InboundDelegateMsg,
    Runtime,
    SecretsStore,
    // ApplicationMessage,
    // SecretsId,
};
use freenet_aft_interface::{
    AllocationCriteria, DelegateParameters, Tier, TokenAllocationRecord, TokenAssignment,
};
use freenet_stdlib::prelude::*;

static TEST_NO: AtomicUsize = AtomicUsize::new(0);

type Assignee = RsaPublicKey;

type AssignmentHash = [u8; 32];

#[derive(Debug, Serialize, Deserialize)]
enum FailureReason {
    /// The user didn't accept to allocate the tokens.
    UserPermissionDenied,
    /// No free slot to allocate with the requested criteria
    NoFreeSlot {
        delegate_id: SecretsId,
        criteria: AllocationCriteria,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestNewToken {
    request_id: u32,
    delegate_id: SecretsId,
    criteria: AllocationCriteria,
    records: TokenAllocationRecord,
    /// The public key
    assignee: Assignee,
    assignment_hash: AssignmentHash,
}

#[derive(Debug, Serialize, Deserialize)]
enum TokenDelegateMessage {
    RequestNewToken(RequestNewToken),
    AllocatedToken {
        delegate_id: SecretsId,
        assignment: TokenAssignment,
        /// An updated version of the record with the newly allocated token included
        records: TokenAllocationRecord,
    },
    Failure(FailureReason),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Allowed,
    NotAllowed,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Context {
    waiting_for_user_input: HashSet<u32>,
    user_response: HashMap<u32, Response>,
}

fn test_dir(prefix: &str) -> PathBuf {
    let test_dir = std::env::temp_dir().join("freenet-test").join(format!(
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
    private_key: &RsaPrivateKey,
    contract_name: &str,
    delegate_name: &str,
) -> Result<(Delegate<'a>, SecretsId, ContractKey, Runtime), Box<dyn std::error::Error>> {
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

    // Setup delegate
    let delegates_dir = test_dir("aft-delegate");
    let secrets_dir = test_dir("aft-secret");

    let mut delegate_store = DelegateStore::new(delegates_dir, 10_000)?;
    let mut secret_store = SecretsStore::new(secrets_dir)?;

    let delegate = {
        let bytes = get_test_module("delegates", delegate_name)?;
        let delegate_params = DelegateParameters::new(private_key.clone());
        let params = Parameters::from(serde_json::to_vec(&delegate_params)?);
        let code = DelegateCode::from(bytes);
        Delegate::from((&code, &params))
    };

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let _ = secret_store.register_delegate(delegate.key().clone(), cipher, nonce);
    let _ = delegate_store.store_delegate(delegate.clone());

    // Store secret token
    let secret_id = SecretsId::new(vec![0, 1, 2]);
    let encoded = bincode::serialize(&private_key.to_public_key()).unwrap();
    secret_store
        .store_secret(&delegate.key().clone(), &secret_id, encoded)
        .unwrap();

    let runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();

    //runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
    Ok((delegate, secret_id, contract_key, runtime))
}

#[test]
fn test_process_allocated_token() {
    // let _ = tracing_subscriber::fmt()
    //     .with_env_filter("error")
    //     .try_init();

    let mut csprng = rand_chacha::ChaChaRng::seed_from_u64(1);
    let private_key = RsaPrivateKey::new(&mut csprng, 4098).unwrap();

    let (delegate, secret_id, contract_key, mut runtime) =
        set_up_aft(&private_key, "token-allocation-record", "token-generator").unwrap();
    let app = ContractInstanceId::try_from(contract_key.to_string()).unwrap();
    let context: Context = Context {
        waiting_for_user_input: HashSet::default(),
        user_response: HashMap::default(),
    };
    let delegate_context = DelegateContext::new(bincode::serialize(&context).unwrap());
    let criteria = AllocationCriteria::new(
        Tier::Day1,
        std::time::Duration::from_secs(365 * 24 * 3600),
        app,
    )
    .unwrap();

    let request_new_token = RequestNewToken {
        request_id: 1,
        delegate_id: secret_id,
        criteria,
        records: TokenAllocationRecord::new(std::collections::HashMap::new()),
        assignee: private_key.to_public_key(),
        assignment_hash: [0; 32],
    };

    let message = TokenDelegateMessage::RequestNewToken(request_new_token);
    let payload: Vec<u8> = bincode::serialize(&message).unwrap();

    // The application request new token allocation
    let inbound_message = InboundDelegateMsg::ApplicationMessage(
        ApplicationMessage::new(app, payload).with_context(delegate_context),
    );

    let delegate_params = DelegateParameters::new(private_key.clone());
    let params = Parameters::from(serde_json::to_vec(&delegate_params).unwrap());
    let outbound = runtime
        .inbound_app_message(delegate.key(), &params, vec![inbound_message])
        .unwrap();
    assert_eq!(outbound.len(), 1);

    let result_msg = match outbound.get(0) {
        Some(OutboundDelegateMsg::ApplicationMessage(ApplicationMessage { payload, .. })) => {
            let msg: TokenDelegateMessage = bincode::deserialize(payload.as_ref()).unwrap();
            msg
        }
        _ => panic!("Unexpected outbound message"),
    };

    match result_msg {
        TokenDelegateMessage::AllocatedToken { assignment, .. } => {
            assert_eq!(assignment.tier, Tier::Day1);
            assert_eq!(
                assignment.token_record,
                ContractInstanceId::from(contract_key)
            );
        }
        _ => panic!("Unexpected token delegate message"),
    }
}
