use clap::Parser;
use tracing_subscriber::EnvFilter;
use byteorder::{BigEndian, WriteBytesExt};
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};

use locutus_runtime::locutus_stdlib::web::model::WebModelState;
use locutus_dev::{LocalNodeConfig, StateConfig, SubCommand, ContractType, set_cleanup_on_exit};
use locutus_dev::{user_fn_handler, wasm_runtime, AppState, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    match Config::parse().sub_command {
        SubCommand::RunLocal(contract_cli) => {
            run_local_node_client(contract_cli).await
        },
        SubCommand::Build(state_cli) => {
            generate_state(state_cli)
        }
    }
}

async fn run_local_node_client(cli: LocalNodeConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    if cli.disable_tui_mode {
        return Err("CLI mode not yet implemented".into());
    }

    if cli.clean_exit {
        set_cleanup_on_exit()?;
    }

    let app_state = AppState::new(&cli).await?;
    let (sender, receiver) = tokio::sync::mpsc::channel(100);
    let runtime = tokio::task::spawn(wasm_runtime(cli.clone(), receiver, app_state.clone()));
    let user_fn = user_fn_handler(cli, sender, app_state);
    tokio::select! {
        res = runtime => { res?? }
        res = user_fn => { res? }
    };
    println!("Shutdown...");
    Ok(())
}

fn generate_state(cli: StateConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut complete_state = Vec::new();

    let source_path: PathBuf = cli.input_path;
    let dest_file: PathBuf = cli.output_file;

    match cli.contract_type {
        ContractType::View => {
            build_view_state(&mut complete_state, source_path, dest_file)?;
        }
        ContractType::Model => {
            build_model_state(source_path, dest_file)?;
        }
    }

    Ok(())
}

fn build_view_state(
    complete_state: &mut Vec<u8>,
    source_path: PathBuf,
    dest_file: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing::debug!("Bundling `view` state from {source_path:?} into {dest_file:?}");
    // FIXME: use instead WebModelState
    append_metadata(complete_state)?;
    append_web_content(complete_state, source_path)?;
    let mut state = File::create(dest_file)?;
    state.write_all(complete_state)?;
    Ok(())
}

fn build_model_state(
    source_path: PathBuf,
    dest_file: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing::debug!("Bundling `model` contract state from {source_path:?} into {dest_file:?}");
    // FIXME: optionally provide a path to the metadata
    let mut model = vec![];
    let mut model_f = File::open(source_path)?;
    model_f.read_to_end(&mut model)?;
    let model = WebModelState::from_data(vec![], model);

    let mut state = File::create(dest_file)?;
    state.write_all(model.pack()?.as_slice())?;
    Ok(())
}

fn append_metadata(state: &mut Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let metadata: &[u8] = &[];
    state.write_u64::<BigEndian>(metadata.len() as u64)?;
    Ok(())
}

fn append_web_content(
    state: &mut Vec<u8>,
    source_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let encoder = xz2::write::XzEncoder::new(Vec::new(), 6);
    let mut tar = tar::Builder::new(encoder);
    tar.append_dir_all(".", &source_path)?;
    let encoder_data= tar.into_inner()?;
    let mut encoded: Vec<u8> = encoder_data.finish()?;
    state.write_u64::<BigEndian>(encoded.len() as u64)?;
    state.append(&mut encoded);
    Ok(())
}
