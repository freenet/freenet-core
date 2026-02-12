use std::fs;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tar::Builder;
use xz2::read::XzEncoder;
use freenet::server::WebApp;
use crate::config::{SyncConfig, BaseConfig};
use crate::commands::{start_api_client, execute_command, close_api_client, RESPONSE_TIMEOUT};
use freenet_stdlib::client_api::{ContractRequest, ContractResponse, HostResponse};
use freenet_stdlib::prelude::*;

pub async fn sync(config: SyncConfig, other: BaseConfig) -> anyhow::Result<()> {
    let sync_path = &config.path;
    let freenet_dir = sync_path.join(".freenet");
    let state_file = freenet_dir.join("state.json");

    if !freenet_dir.exists() {
        println!("Initializing sync directory at {:?}", sync_path);
        fs::create_dir_all(&freenet_dir)?;
    }

    // Load existing contract ID if available
    let contract_id = if state_file.exists() {
        let content = fs::read_to_string(&state_file)?;
        let state: serde_json::Value = serde_json::from_str(&content)?;
        state["contract_id"].as_str().map(|s| s.to_string())
    } else {
        None
    };

    println!("Packing directory {:?}", sync_path);
    let packed_state = pack_directory(sync_path)?;

    // Use our custom sync container
    let container_wasm_path = PathBuf::from("/workspaces/freenet-core/crates/fdev/src/sync/container/target/wasm32-unknown-unknown/release/fdev_sync_container.wasm");
    
    if !container_wasm_path.exists() {
        anyhow::bail!("Container contract WASM not found at {:?}. Please build it first.", container_wasm_path);
    }

    let params = Parameters::from(vec![]);
    // Load contract correctly with version wrapper
    let contract = if let Ok(raw_code) = ContractCode::load_raw(&container_wasm_path) {
        let code = ContractCode::from(raw_code.data().to_vec());
        let wrapped = WrappedContract::new(Arc::new(code), params);
        let api_version = ContractWasmAPIVersion::V1(wrapped);
        ContractContainer::from(api_version)
    } else {
        ContractContainer::try_from((container_wasm_path.as_path(), params))?
    };

    let mut client = start_api_client(other).await?;

    if let Some(ref id) = contract_id {
        println!("Synchronizing with existing contract {}", id);
    }

    println!("Publishing sync state to network...");
    let request = ContractRequest::Put {
        contract,
        state: packed_state.into(),
        related_contracts: Default::default(),
        subscribe: true,
        blocking_subscribe: false,
    }.into();

    execute_command(request, &mut client).await?;

    match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key: response_key }))) => {
            println!("Sync successful! Contract ID: {}", response_key);
            save_state(&state_file, &response_key.to_string())?;
        }
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse { key: response_key, .. }))) => {
            println!("Sync update successful for {}", response_key);
            save_state(&state_file, &response_key.to_string())?;
        }
        Ok(Ok(other)) => anyhow::bail!("Unexpected response: {:?}", other),
        Ok(Err(e)) => anyhow::bail!("Error receiving response: {}", e),
        Err(_) => anyhow::bail!("Timeout waiting for sync response"),
    }

    close_api_client(&mut client).await;
    Ok(())
}

fn save_state(path: &Path, contract_id: &str) -> anyhow::Result<()> {
    let state = serde_json::json!({
        "contract_id": contract_id,
        "last_sync": chrono::Utc::now().to_rfc3339(),
    });
    fs::write(path, serde_json::to_string_pretty(&state)?)?;
    Ok(())
}

pub fn pack_directory(path: &Path) -> anyhow::Result<Vec<u8>> {
    let buf = Cursor::new(Vec::new());
    let mut builder = Builder::new(buf);

    // Recursively add files, excluding .freenet
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        if name == ".freenet" {
            continue;
        }

        if file_type.is_dir() {
            builder.append_dir_all(&name, entry.path())?;
        } else {
            let mut file = fs::File::open(entry.path())?;
            builder.append_file(&name, &mut file)?;
        }
    }

    builder.finish()?;
    let archive_data = builder.into_inner()?.into_inner();
    
    // Compress the archive data using XZ
    let mut encoder = XzEncoder::new(Cursor::new(archive_data), 6);
    let mut compressed = vec![];
    encoder.read_to_end(&mut compressed)?;
    
    // Create a WebApp-compatible state (Metadata + Compressed Tar)
    let webapp = WebApp::from_compressed(vec![], compressed)?;
    let packed = webapp.pack()?;
    
    Ok(packed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_tungstenite::tungstenite::Message;
    use std::net::Ipv4Addr;
    use freenet::dev_tool::OperationMode;
    use xz2::read::XzDecoder;
    use tar::Archive;

    #[test]
    fn test_pack_directory() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path();

        fs::write(path.join("file1.txt"), "content1")?;
        let sub = path.join("sub");
        fs::create_dir(&sub)?;
        fs::write(sub.join("file2.txt"), "content2")?;

        let freenet_dir = path.join(".freenet");
        fs::create_dir(&freenet_dir)?;
        fs::write(freenet_dir.join("secret.json"), "{}")?;

        let packed = pack_directory(path)?;
        assert!(!packed.is_empty());

        // Manually unpack to verify
        let webapp = WebApp::try_from(packed.as_slice())?;
        let decoder = XzDecoder::new(webapp.web.as_slice());
        let mut archive = Archive::new(decoder);
        let unpack_dir = tempdir()?;
        archive.unpack(unpack_dir.path())?;

        assert!(unpack_dir.path().join("file1.txt").exists());
        assert!(unpack_dir.path().join("sub/file2.txt").exists());
        assert!(!unpack_dir.path().join(".freenet").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_command_flow() -> anyhow::Result<()> {
        let port = 55003;
        let dir = tempdir()?;
        let path = dir.path();
        fs::write(path.join("hello.txt"), "world")?;

        // Create a dummy WASM file for the test
        let container_dir = PathBuf::from("/workspaces/freenet-core/crates/fdev/src/sync/container/target/wasm32-unknown-unknown/release");
        fs::create_dir_all(&container_dir)?;
        let container_wasm_path = container_dir.join("fdev_sync_container.wasm");
        if !container_wasm_path.exists() {
            fs::write(&container_wasm_path, vec![0u8; 100])?;
        }

        let instance_id = ContractInstanceId::try_from("GXewqt7p2341CepfoPaQzgFzK4qJCC8S7VsGogG939vg".to_string()).unwrap();
        let mock_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([0u8; 32]));
        let response: HostResponse<WrappedState> = HostResponse::ContractResponse(ContractResponse::PutResponse { key: mock_key.clone() });

        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;

        let server_handle = tokio::spawn(async move {
            let _ = ready_tx.send(());
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
            use futures::{SinkExt, StreamExt};
            let _msg = ws_stream.next().await.unwrap();
            let response_bytes = bincode::serialize(&Ok::<_, freenet_stdlib::client_api::ClientError>(response)).unwrap();
            ws_stream.send(Message::Binary(response_bytes.into())).await.unwrap();
            let _msg = ws_stream.next().await;
        });

        ready_rx.await?;

        let sync_config = SyncConfig { path: path.to_path_buf() };
        let base_config = BaseConfig {
            paths: freenet::config::ConfigPathsArgs::default(),
            mode: OperationMode::Local,
            port,
            address: Ipv4Addr::LOCALHOST.into(),
        };

        sync(sync_config, base_config).await?;

        let state_file = path.join(".freenet/state.json");
        assert!(state_file.exists());
        let content = fs::read_to_string(state_file)?;
        let state: serde_json::Value = serde_json::from_str(&content)?;
        assert_eq!(state["contract_id"], mock_key.to_string());

        server_handle.abort();
        Ok(())
    }
}
