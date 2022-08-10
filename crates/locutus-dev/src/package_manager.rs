use std::{
    fs::File,
    io::{Cursor, Read, Write},
    path::PathBuf,
};

use locutus_runtime::locutus_stdlib::web::{controller::ControllerState, view::WebViewState};
use serde::Deserialize;
use tar::Builder;

use crate::{ContractType, DynError, PackageManagerConfig};

const DEFAULT_OUTPUT_NAME: &str = "contract-state";

// TODO: polish error handling with its own error type
#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Configuration error: {0}")]
    MissConfiguration(&'static str),
}

pub fn package_state(_cli_config: PackageManagerConfig) -> Result<(), DynError> {
    let cwd = std::env::current_dir()?;
    let config_file = cwd.join("locutus.toml");
    if config_file.exists() {
        let mut f_content = vec![];
        File::open(config_file)?.read_to_end(&mut f_content)?;
        let config: PackageConfig = toml::from_slice(&f_content)?;
        internal_package_state(config)
    } else {
        Err("could not locate `locutus.toml` config file in current dir".into())
    }
}

#[derive(Deserialize)]
struct PackageConfig {
    package: Package,
    sources: Sources,
    metadata: Option<PathBuf>,
    output: Option<Output>,
}

#[derive(Deserialize)]
struct Package {
    #[serde(rename(deserialize = "type"))]
    c_type: ContractType,
}

#[derive(Deserialize)]
struct Sources {
    source_dirs: Option<Vec<PathBuf>>,
    files: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct Output {
    path: PathBuf,
}

fn internal_package_state(config: PackageConfig) -> Result<(), DynError> {
    let metadata = if let Some(md) = &config.metadata {
        let mut buf = vec![];
        File::open(md)?.read_to_end(&mut buf)?;
        buf
    } else {
        vec![]
    };

    match config.package.c_type {
        ContractType::View => build_view_state(metadata, config),
        ContractType::Controller => build_controller_state(metadata, config),
    }
}

fn build_view_state(metadata: Vec<u8>, config: PackageConfig) -> Result<(), DynError> {
    println!("Bundling `view` contract state");
    let mut archive: Builder<Cursor<Vec<u8>>> = Builder::new(Cursor::new(Vec::new()));
    let mut found_entry = false;
    if let Some(sources) = &config.sources.files {
        for src in sources {
            for entry in glob::glob(src)? {
                let p = entry?;
                println!("p: {p:?}");
                if p.ends_with("index.html") && p.starts_with("index.html") {
                    // ensures that index is present and at the root
                    found_entry = true;
                }
                let mut f = File::open(&p)?;
                archive.append_file(p, &mut f)?;
            }
        }
    }
    if let Some(src_dirs) = &config.sources.source_dirs {
        for dir in src_dirs {
            if dir.is_dir() {
                let present_entry = dir.join("index.html").exists();
                if !found_entry && present_entry {
                    found_entry = true;
                } else if present_entry {
                    return Err(format!(
                        "duplicate entry point (index.html) found at directory: {dir:?}"
                    )
                    .into());
                }
                archive.append_dir_all(".", &dir)?;
            } else {
                return Err(format!("unknown directory: {dir:?}").into());
            }
        }
    }
    if config.sources.source_dirs.is_none() && config.sources.files.is_none() {
        return Err("need to specify source dirs and/or files".into());
    }
    if !found_entry {
        Err("didn't find entry point `index.html` in package".into())
    } else {
        let state = WebViewState::from_data(metadata, archive)?;
        let packed = state.pack()?;
        output_artifact(&config.output, &packed)?;
        println!("Finished bundling `view` contract state");
        Ok(())
    }
}

fn build_controller_state(metadata: Vec<u8>, config: PackageConfig) -> Result<(), DynError> {
    const REQ_ONE_FILE_ERR: &str = "Requires exactly one source file specified for the state.";

    println!("Bundling `controller` contract state");
    let mut src_files = config
        .sources
        .files
        .ok_or(Error::MissConfiguration(REQ_ONE_FILE_ERR))?;

    let state: PathBuf = (src_files.len() == 1)
        .then(|| src_files.pop().unwrap())
        .ok_or(Error::MissConfiguration(REQ_ONE_FILE_ERR))?
        .into();

    let mut buf = vec![];
    let mut model_f = File::open(state)?;
    model_f.read_to_end(&mut buf)?;
    let controller_state = ControllerState::from_data(metadata, buf);
    let packed = controller_state.pack()?;
    output_artifact(&config.output, &packed)?;
    println!("Finished bundling `controller` contract state");
    Ok(())
}

fn output_artifact(output: &Option<Output>, packed: &[u8]) -> Result<(), DynError> {
    if let Some(output) = output {
        File::create(&output.path)?.write_all(packed)?;
    } else {
        let default_out_dir = std::env::current_dir()?.join("target").join("locutus");
        std::fs::create_dir_all(&default_out_dir)?;
        let mut f = File::create(default_out_dir.join(DEFAULT_OUTPUT_NAME))?;
        f.write_all(packed)?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn package_view_contract() -> Result<(), DynError> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let cwd = PathBuf::from(CRATE_DIR).join("../../apps/freenet-microblogging/view");
        std::env::set_current_dir(cwd)?;

        build_view_state(
            vec![],
            PackageConfig {
                sources: Sources {
                    source_dirs: Some(vec!["web/static".into()]),
                    files: Some(vec!["web/dist/bundle.js".into()]),
                },
                package: Package {
                    c_type: ContractType::View,
                },
                metadata: None,
                output: None,
            },
        )?;

        let mut buf = vec![];
        File::open(
            PathBuf::from("target")
                .join("locutus")
                .join(DEFAULT_OUTPUT_NAME),
        )?
        .read_to_end(&mut buf)?;
        let state = locutus_runtime::locutus_stdlib::interface::State::from(buf);
        let mut view = WebViewState::try_from(state).unwrap();

        let target = std::env::temp_dir().join("locutus-unpack-state");
        let e = view.unpack(&target);
        let unpacked_successfully = target.join("index.html").exists();

        std::fs::remove_dir_all(target)?;
        e?;
        assert!(unpacked_successfully, "failed to unpack state");

        Ok(())
    }

    #[test]
    fn package_controller_contract() -> Result<(), DynError> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let cwd = PathBuf::from(CRATE_DIR).join("../../apps/freenet-microblogging/controller");
        std::env::set_current_dir(cwd)?;

        build_controller_state(
            vec![],
            PackageConfig {
                sources: Sources {
                    source_dirs: None,
                    files: Some(vec!["initial_state.json".into()]),
                },
                package: Package {
                    c_type: ContractType::Controller,
                },
                metadata: None,
                output: None,
            },
        )?;

        assert!(PathBuf::from("target")
            .join("locutus")
            .join(DEFAULT_OUTPUT_NAME)
            .exists());

        Ok(())
    }
}
