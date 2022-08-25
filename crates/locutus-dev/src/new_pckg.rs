use std::{
    env, fs,
    path::{Path, PathBuf},
};

use crate::{build_tool::*, config::NewPackageCliConfig, DynError};

pub fn create_new_package(config: NewPackageCliConfig) -> Result<(), DynError> {
    let cwd = env::current_dir()?;
    match config.kind {
        crate::config::ContractKind::WebView => create_view_package(&cwd)?,
        crate::config::ContractKind::WebController => todo!(),
    }
    Ok(())
}

fn create_view_package(cwd: &Path) -> Result<(), DynError> {
    fs::create_dir_all(cwd.join("container"))?;
    let locutus_file_config = BuildToolConfig {
        contract: Contract {
            c_type: ContractType::View,
            lang: Some(SupportedContractLangs::Rust),
        },
        sources: Some(Sources {
            source_dirs: Some(vec![PathBuf::from("web").join("dist")]),
            files: None,
        }),
        metadata: None,
        output: None,
        webapp: Some(WebAppContract {
            lang: SupportedViewLangs::Typescript,
            typescript: Some(TypescriptConfig { webpack: true }),
        }),
    };
    Ok(())
}
