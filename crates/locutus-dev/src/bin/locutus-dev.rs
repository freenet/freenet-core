use clap::Parser;
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};
use tracing_subscriber::EnvFilter;

use locutus_dev::{set_cleanup_on_exit, ContractType, LocalNodeConfig, StateConfig, SubCommand};
use locutus_dev::{user_fn_handler, wasm_runtime, AppState, Config};
use locutus_runtime::locutus_stdlib::web::model::WebModelState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    match Config::parse().sub_command {
        SubCommand::RunLocal(contract_cli) => run_local_node_client(contract_cli).await,
        SubCommand::Build(state_cli) => generate_state(state_cli),
    }
}

async fn run_local_node_client(
    cli: LocalNodeConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
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

fn generate_state(
    cli: StateConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let metadata_path: Option<PathBuf> = cli.input_metadata_path;
    let state_path: PathBuf = cli.input_state_path;
    let dest_file: PathBuf = cli.output_file;

    match cli.contract_type {
        ContractType::View => {
            build_view_state(metadata_path, state_path, dest_file)?;
        }
        ContractType::Model => {
            build_model_state(metadata_path, state_path, dest_file)?;
        }
    }

    Ok(())
}

fn build_view_state(
    metadata_path: Option<PathBuf>,
    state_path: PathBuf,
    dest_file: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing::debug!("Bundling `view` state from {state_path:?} into {dest_file:?}");

    let mut metadata = vec![];

    if let Some(path) = metadata_path {
        let mut metadata_f = File::open(path)?;
        metadata_f.read_to_end(&mut metadata)?;
    }

    let view = get_encoded_view(state_path).expect("Failed encoding view");
    let model = WebModelState::from_data(metadata, view);

    let mut state = File::create(dest_file)?;
    state.write_all(model.pack()?.as_slice())?;
    Ok(())
}

fn build_model_state(
    metadata_path: Option<PathBuf>,
    state_path: PathBuf,
    dest_file: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
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

fn get_encoded_view(
    view_path: PathBuf,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut encoded_view: Vec<u8> = vec![];
    let encoder = xz2::write::XzEncoder::new(Vec::new(), 6);
    let mut tar = tar::Builder::new(encoder);
    tar.append_dir_all(".", &view_path)?;
    let encoder_data = tar.into_inner()?;
    let mut encoded: Vec<u8> = encoder_data.finish()?;
    encoded_view.append(&mut encoded);
    Ok(encoded_view)
}
