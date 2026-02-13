use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use freenet::server::WebApp;
use freenet_stdlib::client_api::{ContractRequest, ContractResponse, HostResponse};
use freenet_stdlib::prelude::*;
use ignore::WalkBuilder;
use notify::{Event, RecursiveMode, Watcher};
use tokio::sync::mpsc;

use crate::commands::{close_api_client, execute_command, start_api_client, RESPONSE_TIMEOUT};
use crate::config::{BaseConfig, SyncConfig};

/// The WASM contract code for the sync container.
const SYNC_CONTRACT_CODE: &[u8] = include_bytes!("sync/container.wasm");

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct SyncState {
    contract_id: String,
    last_sync: u64,
}

pub async fn sync(config: SyncConfig, other: BaseConfig) -> Result<()> {
    let path = config
        .path
        .canonicalize()
        .context("Failed to canonicalize path")?;
    if !path.is_dir() {
        anyhow::bail!("Path must be a directory: {}", path.display());
    }

    println!("Synchronizing directory: {}", path.display());

    // Load or deploy contract
    let state_file = path.join(".freenet").join("state.json");
    let contract_key = if state_file.exists() {
        let content = fs::read_to_string(&state_file)?;
        let state: SyncState = serde_json::from_str(&content)?;
        println!("Using existing contract: {}", state.contract_id);
        let instance_id = ContractInstanceId::try_from(state.contract_id)?;
        ContractKey::from_id_and_code(instance_id, CodeHash::new([0u8; 32]))
    } else {
        println!("Deploying new sync contract...");
        let key = deploy_contract(&path, &other).await?;

        // Save state
        let state_dir = path.join(".freenet");
        fs::create_dir_all(&state_dir)?;
        let state = SyncState {
            contract_id: key.to_string(),
            last_sync: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        };
        fs::write(&state_file, serde_json::to_string_pretty(&state)?)?;
        println!("Contract deployed: {}", state.contract_id);
        key
    };

    // Initial sync
    println!("Performing initial synchronization...");
    update_contract(&path, &contract_key, &other).await?;
    println!("Initial synchronization complete.");

    if config.watch {
        println!("Watching for changes (Ctrl+C to stop)...");
        watch_directory(&path, &contract_key, &other).await?;
    }

    Ok(())
}

async fn deploy_contract(path: &Path, other: &BaseConfig) -> Result<ContractKey> {
    let code = ContractCode::from(SYNC_CONTRACT_CODE.to_vec());
    let params = Parameters::from(vec![]);
    let wrapped = WrappedContract::new(Arc::new(code), params);
    let api_version = ContractWasmAPIVersion::V1(wrapped);
    let contract = ContractContainer::from(api_version);
    let key = contract.key();

    let state = pack_directory(path)?;
    let request = ContractRequest::Put {
        contract,
        state: state.into(),
        related_contracts: Default::default(),
        subscribe: true,
        blocking_subscribe: false,
    }
    .into();

    let mut client = start_api_client(other.clone()).await?;
    execute_command(request, &mut client).await?;

    match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { .. }))) => {
            close_api_client(&mut client).await;
            Ok(key)
        }
        Ok(Ok(other)) => {
            close_api_client(&mut client).await;
            anyhow::bail!("Unexpected response during deployment: {:?}", other);
        }
        Ok(Err(e)) => {
            close_api_client(&mut client).await;
            anyhow::bail!("Connection error during deployment: {}", e);
        }
        Err(_) => {
            close_api_client(&mut client).await;
            anyhow::bail!("Timeout waiting for deployment response");
        }
    }
}

async fn update_contract(path: &Path, key: &ContractKey, other: &BaseConfig) -> Result<()> {
    let state = pack_directory(path)?;
    let request = ContractRequest::Update {
        key: *key,
        data: UpdateData::Delta(StateDelta::from(state)),
    }
    .into();

    let mut client = start_api_client(other.clone()).await?;
    execute_command(request, &mut client).await?;

    match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse { .. }))) => {
            close_api_client(&mut client).await;
            Ok(())
        }
        Ok(Ok(other)) => {
            close_api_client(&mut client).await;
            anyhow::bail!("Unexpected response during update: {:?}", other);
        }
        Ok(Err(e)) => {
            close_api_client(&mut client).await;
            anyhow::bail!("Connection error during update: {}", e);
        }
        Err(_) => {
            close_api_client(&mut client).await;
            anyhow::bail!("Timeout waiting for update response");
        }
    }
}

fn pack_directory(path: &Path) -> Result<Vec<u8>> {
    let mut builder = WalkBuilder::new(path);
    builder.hidden(false); // Process hidden files but we will skip .freenet
    let walker = builder.build();

    let mut tar_builder = tar::Builder::new(Vec::new());

    for entry in walker {
        let entry = entry?;
        let entry_path = entry.path();

        // Skip .freenet directory to avoid infinite loops and metadata leakage
        if entry_path.components().any(|c| c.as_os_str() == ".freenet") {
            continue;
        }

        if entry_path.is_file() {
            let rel_path = entry_path.strip_prefix(path)?;
            let mut file = File::open(entry_path)?;
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;

            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_path(rel_path)?;
            header.set_mode(0o644);
            tar_builder.append(&header, &content[..])?;
        }
    }

    let tar_data = tar_builder.into_inner()?;

    // Compress with XZ
    let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 6);
    encoder.write_all(&tar_data)?;
    let compressed_tar = encoder.finish()?;

    // Wrap in WebApp state
    let webapp = WebApp::from_compressed(vec![], compressed_tar)?;
    Ok(webapp.pack()?)
}

async fn watch_directory(path: &Path, key: &ContractKey, other: &BaseConfig) -> Result<()> {
    let (tx, mut rx) = mpsc::channel(100);

    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        if let Ok(event) = res {
            // Only trigger on data-changing events
            if event.kind.is_modify() || event.kind.is_create() || event.kind.is_remove() {
                let _ = tx.blocking_send(());
            }
        }
    })?;

    watcher.watch(path, RecursiveMode::Recursive)?;

    let debounce_duration = Duration::from_secs(2);
    let mut last_update = Instant::now();
    let mut pending = false;

    loop {
        tokio::select! {
            Some(_) = rx.recv() => {
                pending = true;
            }
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                if pending && last_update.elapsed() >= debounce_duration {
                    println!("Change detected, updating Freenet contract...");
                    if let Err(e) = update_contract(path, key, other).await {
                        eprintln!("Warning: Failed to update contract: {}", e);
                    } else {
                        println!("Contract updated successfully.");
                    }
                    pending = false;
                    last_update = Instant::now();
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Stopping directory watch.");
                break;
            }
        }
    }

    Ok(())
}
