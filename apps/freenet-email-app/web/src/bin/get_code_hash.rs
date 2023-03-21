use std::env;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use locutus_stdlib::prelude::Parameters;

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");

fn main() {
    use locutus_dev::config::BuildToolCliConfig;
    use locutus_stdlib::prelude::ContractContainer;

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <key-id> <path-to-inbox-state>", args[0]);
        std::process::exit(1);
    }
    println!("args: {:?}", args);
    let key_id = &args[1];
    let state_path = &args[2];

    // Open the state file and read params content
    let mut state_file = File::open(state_path).unwrap();
    let mut buffer = Vec::new();
    state_file.read_to_end(&mut buffer).unwrap();
    let params = Parameters::from(buffer);

    let config = BuildToolCliConfig::default();
    let contract_path = PathBuf::from(MANIFEST).join("../contracts/inbox");
    eprintln!("inbox path: {contract_path:?}");
    let contract_path = contract_path.canonicalize().unwrap();
    locutus_dev::build::build_package(config, contract_path.as_path()).unwrap();
    let f = contract_path.join("build/locutus/freenet_email_inbox");
    eprintln!("wasm path: {f:?}");
    let code_key = ContractContainer::try_from((f.as_path(), params))
        .unwrap()
        .unwrap_v1()
        .code()
        .hash_str();
    eprintln!("Inbox code hash: {code_key}");
    std::fs::write(
        PathBuf::from(MANIFEST).join(format!("examples/inbox_code_hash_{}", key_id)),
        code_key,
    )
        .unwrap();
}