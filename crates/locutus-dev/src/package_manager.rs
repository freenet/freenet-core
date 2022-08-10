use std::{
    fs::File,
    io::{Cursor, Read, Write},
    path::PathBuf,
};

use locutus_runtime::locutus_stdlib::web::{model::WebModelState, view::WebViewState};
use serde::Deserialize;
use tar::Builder;

use crate::{ContractType, DynError, PackageManagerConfig};

// TODO: polish error handling with its own error type

const DEFAULT_OUTPUT_NAME: &str = "contract-state";

pub fn package_state(cli_config: PackageManagerConfig) -> Result<(), DynError> {
    let cwd = std::env::current_dir()?;
    let config_file = cwd.join("locutus.toml");
    if config_file.exists() {
        let mut f_content = vec![];
        File::open(config_file)?.read_to_end(&mut f_content)?;
        let config: PackageConfig = toml::from_slice(&f_content)?;
        internal_package_state(config, cli_config.contract_type)
    } else {
        Err("could not locate `locutus.toml` config file in current dir".into())
    }
}

#[derive(Deserialize)]
struct PackageConfig {
    sources: Sources,
    metadata: Option<PathBuf>,
    output: Option<Output>,
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

fn internal_package_state(
    config: PackageConfig,
    contract_type: ContractType,
) -> Result<(), DynError> {
    let metadata = if let Some(md) = &config.metadata {
        let mut buf = vec![];
        File::open(md)?.read_to_end(&mut buf)?;
        buf
    } else {
        vec![]
    };

    match contract_type {
        ContractType::View => build_view_state(metadata, config),
        ContractType::Model => Ok(()),
    }
}

fn build_view_state(metadata: Vec<u8>, config: PackageConfig) -> Result<(), DynError> {
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
        if let Some(output) = config.output {
            File::create(output.path)?.write_all(&packed)?;
        } else {
            let default_out_dir = std::env::current_dir()?.join("target").join("locutus");
            std::fs::create_dir_all(&default_out_dir)?;
            let mut f = File::create(default_out_dir.join(DEFAULT_OUTPUT_NAME))?;
            f.write_all(&packed)?;
        }
        Ok(())
    }
}

fn build_model_state(
    metadata_path: Option<PathBuf>,
    state_path: PathBuf,
    dest_file: PathBuf,
) -> Result<(), DynError> {
    tracing::debug!("Bundling `model` contract state from {state_path:?} into {dest_file:?}");

    let mut metadata = vec![];
    let mut model = vec![];

    if let Some(path) = metadata_path {
        let mut metadata_f = File::open(path)?;
        metadata_f.read_to_end(&mut metadata)?;
    }

    let mut model_f = File::open(state_path)?;
    model_f.read_to_end(&mut model)?;
    let model = WebModelState::from_data(metadata, model);

    let mut state = File::create(dest_file)?;
    state.write_all(model.pack()?.as_slice())?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn changes() -> Result<(), DynError> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let cwd = PathBuf::from(CRATE_DIR).join("../../contracts/freenet-microblogging/view");
        std::env::set_current_dir(cwd)?;

        build_view_state(
            vec![],
            PackageConfig {
                sources: Sources {
                    source_dirs: Some(vec!["web".into()]),
                    files: Some(vec!["dist/bundle.js".into()]),
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
}
