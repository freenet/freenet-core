use std::path::PathBuf;

use freenet_stdlib::prelude::{ContractCode, ContractKey, DelegateCode, Parameters};

use crate::Error;

/// Inspect a contract, delegate or freenet core compatible executable code properties.
#[derive(clap::Parser, Clone)]
pub struct InspectConfig {
    #[clap(subcommand)]
    r#type: FileType,
    file: PathBuf,
}

#[derive(clap::Subcommand, Clone)]
enum FileType {
    Code(CodeInspection),
    Key,
    Delegate,
}

/// Inspect the packaged WASM code for Freenet.
#[derive(clap::Parser, Clone)]
struct CodeInspection {}

pub fn inspect(config: InspectConfig) -> Result<(), anyhow::Error> {
    if !config.file.exists() {
        return Err(Error::CommandFailed("couldn't find file").into());
    }

    match config.r#type {
        FileType::Code(_) => {
            let (code, version) = ContractCode::load_versioned_from_path(&config.file)?;
            let hash = code.hash_str();
            println!(
                r#"code hash: {hash}
contract API version: {version}
"#
            );
        }
        FileType::Key => {
            let (code, version) = ContractCode::load_versioned_from_path(&config.file)?;
            let hash = code.hash_str();
            let params: Parameters = vec![].into();
            let key = ContractKey::from_params(hash.clone(), params)?;
            println!(
                r#"code key: {key}
contract API version: {version}
"#
            );
        }
        FileType::Delegate => {
            let (code, version) = DelegateCode::load_versioned_from_path(&config.file)?;
            let hash = code.hash_str();
            println!(
                r#"code hash: {hash}
delegate API version: {version}
"#
            );
        }
    }

    Ok(())
}
