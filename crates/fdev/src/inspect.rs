use std::path::PathBuf;

use freenet_stdlib::prelude::{ContractCode, DelegateCode};

use crate::{DynError, Error};

/// Inspect a contract, delegate or locutus kernel compatible executable code properties.
#[derive(clap::Parser, Clone)]
pub struct InspectCliConfig {
    #[clap(subcommand)]
    r#type: FileType,
    file: PathBuf,
}

#[derive(clap::Subcommand, Clone)]
enum FileType {
    Code(CodeInspection),
    Delegate,
    Contract,
}

/// Inspect the packaged WASM code for Locutus.
#[derive(clap::Parser, Clone)]
struct CodeInspection {}

pub fn inspect(config: InspectCliConfig) -> Result<(), DynError> {
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
        FileType::Delegate => {
            let (code, version) = DelegateCode::load_versioned_from_path(&config.file)?;
            let hash = code.hash_str();
            println!(
                r#"code hash: {hash}
delegate API version: {version}
"#
            );
        }
        FileType::Contract => todo!(),
    }

    Ok(())
}
