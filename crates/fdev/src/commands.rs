use std::{fs::File, io::Read, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use freenet::{dev_tool::OperationMode, server::WebApp};
use freenet_stdlib::prelude::{
    ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, WrappedContract,
};
use freenet_stdlib::{
    client_api::{
        ClientRequest, ContractRequest, ContractResponse, DelegateRequest, HostResponse, WebApi,
    },
    prelude::*,
};
use xz2::read::XzDecoder;

use crate::config::{BaseConfig, PutConfig, UpdateConfig};

mod v1;

/// Timeout for waiting for server responses.
/// Contract operations (especially updates with large state) may take time to process
/// and propagate, so we use a generous timeout.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, clap::Subcommand)]
pub(crate) enum PutType {
    /// Publish a new contract to the network
    Contract(PutContract),
    /// Publish a new delegate to the network
    Delegate(PutDelegate),
}

#[derive(clap::Parser, Clone, Debug)]
pub(crate) struct PutContract {
    /// Path to a file listing the related contracts
    #[arg(long)]
    pub(crate) related_contracts: Option<PathBuf>,
    /// Path to the initial state for the contract (typically binary format)
    #[arg(long)]
    pub(crate) state: Option<PathBuf>,
    /// Path to a pre-compressed tar.xz webapp archive containing the webapp files (must include index.html at root)
    #[arg(long)]
    pub(crate) webapp_archive: Option<PathBuf>,
    /// Path to the metadata file to include with the webapp (can be any binary format)
    #[arg(long)]
    pub(crate) webapp_metadata: Option<PathBuf>,
}

#[derive(clap::Parser, Clone, Debug)]
pub(crate) struct PutDelegate {
    /// Base58 encoded nonce for delegate encryption. If empty the default value will be used (only allowed in local mode)
    #[arg(long, env = "DELEGATE_NONCE", default_value_t = String::new())]
    pub(crate) nonce: String,
    /// Base58 encoded cipher for delegate encryption. If empty the default value will be used (only allowed in local mode)
    #[arg(long, env = "DELEGATE_CIPHER", default_value_t = String::new())]
    pub(crate) cipher: String,
}

pub async fn put(config: PutConfig, other: BaseConfig) -> anyhow::Result<()> {
    if config.release {
        anyhow::bail!("Cannot publish contracts in the network yet");
    }
    let params = if let Some(params) = &config.parameters {
        let mut buf = vec![];
        File::open(params)?.read_to_end(&mut buf)?;
        Parameters::from(buf)
    } else {
        Parameters::from(&[] as &[u8])
    };
    match &config.package_type {
        PutType::Contract(contract) => put_contract(&config, contract, other, params).await,
        PutType::Delegate(delegate) => put_delegate(&config, delegate, other, params).await,
    }
}

async fn put_contract(
    config: &PutConfig,
    contract_config: &PutContract,
    other: BaseConfig,
    params: Parameters<'static>,
) -> anyhow::Result<()> {
    // Try to load as raw WASM first
    let contract = if let Ok(raw_code) = ContractCode::load_raw(&config.code) {
        // Add version wrapper
        let code = ContractCode::from(raw_code.data().to_vec());
        let wrapped = WrappedContract::new(Arc::new(code), params);
        let api_version = ContractWasmAPIVersion::V1(wrapped);
        ContractContainer::from(api_version)
    } else {
        // Fall back to trying as already versioned
        ContractContainer::try_from((config.code.as_path(), params))?
    };
    let state = if let Some(ref webapp_archive) = contract_config.webapp_archive {
        // Read pre-compressed webapp archive
        let mut archive = vec![];
        File::open(webapp_archive)?.read_to_end(&mut archive)?;

        // Read optional metadata
        let metadata = if let Some(ref metadata_path) = contract_config.webapp_metadata {
            let mut buf = vec![];
            File::open(metadata_path)?.read_to_end(&mut buf)?;
            buf
        } else {
            vec![]
        };

        // Validate archive has index.html (warning only)
        use std::io::Cursor;
        use tar::Archive;
        let mut found_index = false;
        let decoder = XzDecoder::new(Cursor::new(&archive));
        let mut tar = Archive::new(decoder);
        let entries = tar.entries()?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path()?;
            tracing::debug!("Found file in archive: {}", path.display());
            if path.file_name().map(|f| f.to_string_lossy()) == Some("index.html".into()) {
                tracing::debug!("Found index.html at path: {}", path.display());
                found_index = true;
                break;
            }
        }
        if !found_index {
            tracing::warn!("Warning: No index.html found at root of webapp archive");
        }

        // Create WebApp state from pre-compressed archive
        let webapp = WebApp::from_compressed(metadata.clone(), archive)?;
        tracing::info!(
            metadata_len = metadata.len(),
            "Metadata being packed into WebApp state"
        );
        if !metadata.is_empty() {
            tracing::info!(
                first_32_bytes = format!("{:02x?}", &metadata[..metadata.len().min(32)]),
                "First 32 bytes of metadata"
            );
        }
        let packed = webapp.pack()?;
        tracing::info!(packed_len = packed.len(), "WebApp state after packing");
        if !packed.is_empty() {
            tracing::info!(
                first_32_bytes = format!("{:02x?}", &packed[..packed.len().min(32)]),
                "First 32 bytes of packed state"
            );
        }
        packed.into()
    } else if let Some(ref state_path) = contract_config.state {
        let mut buf = vec![];
        File::open(state_path)?.read_to_end(&mut buf)?;
        buf.into()
    } else {
        tracing::warn!("no state provided for contract, if your contract cannot handle empty state correctly, this will always cause an error.");
        freenet_stdlib::prelude::State::from(vec![])
    };
    let related_contracts: freenet_stdlib::prelude::RelatedContracts =
        if let Some(_related) = &contract_config.related_contracts {
            todo!("use `related` contracts")
        } else {
            Default::default()
        };

    let key = contract.key();
    println!("Publishing contract {key}");
    tracing::debug!(
        state_size = state.as_ref().len(),
        has_related = related_contracts.states().next().is_some(), // FIXME: Should have a better way to test whether there are related contracts
        "Contract details"
    );
    let request = ContractRequest::Put {
        contract,
        state: state.to_vec().into(),
        related_contracts,
        subscribe: config.subscribe,
    }
    .into();
    tracing::debug!("Starting WebSocket client connection");
    let mut client = start_api_client(other).await?;
    tracing::debug!("WebSocket client connected successfully");
    execute_command(request, &mut client).await?;

    // Wait for server response before closing connection (with timeout)
    let result = match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse {
            key: response_key,
        }))) => {
            tracing::info!(%response_key, "Contract published successfully");
            Ok(())
        }
        Ok(Ok(HostResponse::ContractResponse(other))) => {
            Err(anyhow::anyhow!("Unexpected contract response: {:?}", other))
        }
        Ok(Ok(other)) => Err(anyhow::anyhow!("Unexpected response type: {:?}", other)),
        Ok(Err(e)) => Err(anyhow::anyhow!("Failed to receive response: {e}")),
        Err(_) => Err(anyhow::anyhow!(
            "Timeout waiting for server response after {} seconds. \
             The operation may have succeeded - check server logs.",
            RESPONSE_TIMEOUT.as_secs()
        )),
    };

    // Always gracefully close the WebSocket connection, even on timeout/error
    close_api_client(&mut client).await;

    result
}

async fn put_delegate(
    config: &PutConfig,
    delegate_config: &PutDelegate,
    other: BaseConfig,
    params: Parameters<'static>,
) -> anyhow::Result<()> {
    let delegate = DelegateContainer::try_from((config.code.as_path(), params))?;

    let (cipher, nonce) = if delegate_config.cipher.is_empty() && delegate_config.nonce.is_empty() {
        println!(
"Using default cipher and nonce.
For additional hardening is recommended to use a different cipher and nonce to encrypt secrets in storage.");
        (
            ::freenet_stdlib::client_api::DelegateRequest::DEFAULT_CIPHER,
            ::freenet_stdlib::client_api::DelegateRequest::DEFAULT_NONCE,
        )
    } else {
        let mut cipher = [0; 32];
        bs58::decode(delegate_config.cipher.as_bytes())
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .onto(&mut cipher)?;

        let mut nonce = [0; 24];
        bs58::decode(delegate_config.nonce.as_bytes())
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .onto(&mut nonce)?;
        (cipher, nonce)
    };

    let delegate_key = delegate.key().clone();
    println!("Putting delegate {} ", delegate_key.encode());
    let request = DelegateRequest::RegisterDelegate {
        delegate,
        cipher,
        nonce,
    }
    .into();
    let mut client = start_api_client(other).await?;
    execute_command(request, &mut client).await?;

    // Wait for server response before closing connection (with timeout)
    let result = match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::DelegateResponse { key, values })) => {
            tracing::info!(%key, response_count = values.len(), "Delegate registered successfully");
            Ok(())
        }
        Ok(Ok(other)) => Err(anyhow::anyhow!("Unexpected response type: {:?}", other)),
        Ok(Err(e)) => Err(anyhow::anyhow!("Failed to receive response: {e}")),
        Err(_) => Err(anyhow::anyhow!(
            "Timeout waiting for server response after {} seconds. \
             The operation may have succeeded - check server logs.",
            RESPONSE_TIMEOUT.as_secs()
        )),
    };

    // Always gracefully close the WebSocket connection, even on timeout/error
    close_api_client(&mut client).await;

    result
}

#[derive(clap::Parser, Clone, Debug)]
pub(crate) struct GetContractIdConfig {
    /// Path to the contract code (WASM file)
    #[arg(long)]
    pub(crate) code: PathBuf,

    /// Path to the parameters file
    #[arg(long)]
    pub(crate) parameters: Option<PathBuf>,
}

pub async fn get_contract_id(config: GetContractIdConfig) -> anyhow::Result<()> {
    let params = if let Some(params) = &config.parameters {
        let mut buf = vec![];
        File::open(params)?.read_to_end(&mut buf)?;
        Parameters::from(buf)
    } else {
        Parameters::from(&[] as &[u8])
    };

    let contract = if let Ok(raw_code) = ContractCode::load_raw(&config.code) {
        let code = ContractCode::from(raw_code.data().to_vec());
        let wrapped = WrappedContract::new(Arc::new(code), params);
        let api_version = ContractWasmAPIVersion::V1(wrapped);
        ContractContainer::from(api_version)
    } else {
        ContractContainer::try_from((config.code.as_path(), params))?
    };

    let key = contract.key();
    tracing::info!("{key}");
    Ok(())
}

pub async fn update(config: UpdateConfig, other: BaseConfig) -> anyhow::Result<()> {
    if config.release {
        anyhow::bail!("Cannot publish contracts in the network yet");
    }
    // Create ContractKey with placeholder code hash - the node will look up the actual key
    let instance_id = ContractInstanceId::try_from(config.key)?;
    let key = ContractKey::from_id_and_code(instance_id, CodeHash::new([0u8; 32]));
    println!("Updating contract {key}");
    let data = {
        let mut buf = vec![];
        File::open(&config.delta)?.read_to_end(&mut buf)?;
        StateDelta::from(buf).into()
    };
    let request = ContractRequest::Update { key, data }.into();
    let mut client = start_api_client(other).await?;
    execute_command(request, &mut client).await?;

    // Wait for server response before closing connection (with timeout)
    let result = match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key: response_key,
            summary,
        }))) => {
            tracing::info!(%response_key, ?summary, "Contract updated successfully");
            Ok(())
        }
        Ok(Ok(HostResponse::ContractResponse(other))) => {
            Err(anyhow::anyhow!("Unexpected contract response: {:?}", other))
        }
        Ok(Ok(other)) => Err(anyhow::anyhow!("Unexpected response type: {:?}", other)),
        Ok(Err(e)) => Err(anyhow::anyhow!("Failed to receive response: {e}")),
        Err(_) => Err(anyhow::anyhow!(
            "Timeout waiting for server response after {} seconds. \
             The operation may have succeeded - check server logs.",
            RESPONSE_TIMEOUT.as_secs()
        )),
    };

    // Always gracefully close the WebSocket connection, even on timeout/error
    close_api_client(&mut client).await;

    result
}

pub(crate) async fn start_api_client(cfg: BaseConfig) -> anyhow::Result<WebApi> {
    v1::start_api_client(cfg).await
}

/// Gracefully close the WebSocket connection.
/// This sends a Disconnect message and waits briefly for the close handshake to complete,
/// preventing "Connection reset without closing handshake" errors on the server.
pub(crate) async fn close_api_client(client: &mut WebApi) {
    // Send disconnect message - ignore errors since we're closing anyway
    let _ = client.send(ClientRequest::Disconnect { cause: None }).await;
    // Brief delay to allow the close handshake to complete
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

pub(crate) async fn execute_command(
    request: ClientRequest<'static>,
    api_client: &mut WebApi,
) -> anyhow::Result<()> {
    tracing::debug!("Starting execute_command with request: {request}");
    tracing::debug!("Sending request to server and waiting for response...");
    match v1::execute_command(request, api_client).await {
        Ok(_) => {
            tracing::debug!("Server confirmed successful execution");
            Ok(())
        }
        Err(e) => {
            tracing::error!("Server returned error: {}", e);
            Err(e)
        }
    }
}
