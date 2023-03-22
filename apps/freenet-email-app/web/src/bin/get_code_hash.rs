use std::path::PathBuf;

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");

fn main() {
    use locutus_dev::config::BuildToolCliConfig;
    use locutus_stdlib::prelude::ContractContainer;
    let config = BuildToolCliConfig::default();
    let contract_path = PathBuf::from(MANIFEST).join("../contracts/inbox");
    eprintln!("inbox path: {contract_path:?}");
    let contract_path = contract_path.canonicalize().unwrap();
    locutus_dev::build::build_package(config, contract_path.as_path()).unwrap();
    let f = contract_path.join("build/locutus/freenet_email_inbox");
    eprintln!("wasm path: {f:?}");
    let code_key = ContractContainer::try_from((f.as_path(), Vec::<u8>::new().into()))
        .unwrap()
        .unwrap_v1()
        .code()
        .hash_str();
    eprintln!("Inbox code hash: {code_key}");
    std::fs::write(
        PathBuf::from(MANIFEST).join("examples/inbox_code_hash"),
        code_key,
    )
        .unwrap();
}