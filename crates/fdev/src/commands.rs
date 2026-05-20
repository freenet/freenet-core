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

use crate::config::{BaseConfig, GetConfig, PutConfig, SubscribeConfig, UpdateConfig};

mod v1;

/// Timeout for waiting for server responses.
/// Contract operations (especially updates with large state) may take time to process
/// and propagate through the network. Network publish operations may require forwarding
/// through multiple hops, so we use a generous timeout.
pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(300);

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

/// Load a contract code file for `Put` / `get-contract-id` and wrap
/// it in a `ContractContainer` ready to ship over the WS API.
///
/// Distinguishes raw WASM (magic at offset 0) from a packaged contract
/// emitted by `fdev build` (8-byte version + 32-byte hash header,
/// WASM magic at offset 40). Without this discrimination a packaged
/// file flows through `ContractCode::load_raw` (which is just
/// `read_to_end`), the metadata bytes get folded into
/// `ContractCode.data`, and the receiving node hands them to wasmtime
/// as-is. wasmtime's name-section parser then fails UTF-8 validation
/// on the metadata bytes and returns
/// `compile: input bytes aren't valid utf-8`.
///
/// Mirror of #2924 / #3012 — that fix covered the disk-fetch path
/// (`fetch_contract` now strips the prefix via
/// `load_versioned_from_path`); this is the symmetric fix on the
/// publish-input path. Both call sites (`put_contract` and
/// `get_contract_id`) had the same wrong precedence and now route
/// through this helper.
///
/// # Errors
///
/// Returns `Err` if the file cannot be read at all, or if the bytes
/// look packaged but `ContractCode::load_versioned_from_path` rejects
/// them (truncated header, unsupported API version, etc.). An empty
/// file surfaces the underlying read error rather than silently
/// producing a malformed contract.
fn load_contract_for_publish(
    code_path: &std::path::Path,
    params: Parameters<'static>,
) -> anyhow::Result<ContractContainer> {
    const WASM_MAGIC: &[u8; 4] = b"\0asm";
    let container = match ContractCode::load_raw(code_path) {
        Ok(raw_code) if raw_code.data().starts_with(WASM_MAGIC) => {
            let code = ContractCode::from(raw_code.data().to_vec());
            let wrapped = WrappedContract::new(Arc::new(code), params);
            let api_version = ContractWasmAPIVersion::V1(wrapped);
            ContractContainer::from(api_version)
        }
        _ => ContractContainer::try_from((code_path, params))?,
    };
    Ok(container)
}

async fn put_contract(
    config: &PutConfig,
    contract_config: &PutContract,
    other: BaseConfig,
    params: Parameters<'static>,
) -> anyhow::Result<()> {
    let contract = load_contract_for_publish(&config.code, params)?;
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
        tracing::warn!(
            "no state provided for contract, if your contract cannot handle empty state correctly, this will always cause an error."
        );
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
        blocking_subscribe: false,
    }
    .into();
    tracing::debug!("Starting WebSocket client connection");
    let mut client = start_api_client(other).await?;
    tracing::debug!("WebSocket client connected successfully");
    execute_command(request, &mut client).await?;

    // Wait for server response before closing connection (with timeout)
    tracing::info!(
        %key,
        "Request submitted, waiting for network response (timeout: {}s)...",
        RESPONSE_TIMEOUT.as_secs()
    );

    let result = match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse {
            key: response_key,
        }))) => {
            tracing::info!(%response_key, "Contract published successfully");
            Ok(())
        }
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key: response_key,
            ..
        }))) => {
            // When updating an existing contract, server returns UpdateResponse
            tracing::info!(%response_key, "Contract updated successfully");
            Ok(())
        }
        Ok(Ok(HostResponse::ContractResponse(other))) => {
            Err(anyhow::anyhow!("Unexpected contract response: {:?}", other))
        }
        Ok(Ok(other)) => Err(anyhow::anyhow!("Unexpected response type: {:?}", other)),
        Ok(Err(e)) => Err(anyhow::anyhow!("Failed to receive response: {e}")),
        Err(_) => Err(anyhow::anyhow!(
            "Timeout waiting for server response for contract {key} after {} seconds.\n\
             The operation may have succeeded on the network - check server logs.\n\
             If this timeout is consistently too short, consider:\n\
             - Network latency between you and the gateway\n\
             - Contract size and propagation time through the network",
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
        // freenet-stdlib 0.8.0 removed the public `DEFAULT_CIPHER` /
        // `DEFAULT_NONCE` constants (they seeded a world-known key).
        // Generate a fresh random cipher per fdev invocation. The nonce
        // field on the wire is unused by servers running freenet-core
        // >= 0.2.59 (per-write nonces landed in PR #4143); send zeros.
        use rand::RngCore;
        let mut cipher = [0u8; 32];
        rand::rng().fill_bytes(&mut cipher);
        println!(
            "No --cipher specified; generated a fresh random delegate cipher for this \
             session. Pass --cipher / --nonce explicitly to reuse a key across runs."
        );
        (cipher, [0u8; 24])
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
    tracing::info!(
        delegate = %delegate_key.encode(),
        "Request submitted, waiting for network response (timeout: {}s)...",
        RESPONSE_TIMEOUT.as_secs()
    );

    let result = match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::DelegateResponse { key, values })) => {
            tracing::info!(%key, response_count = values.len(), "Delegate registered successfully");
            Ok(())
        }
        Ok(Ok(other)) => Err(anyhow::anyhow!("Unexpected response type: {:?}", other)),
        Ok(Err(e)) => Err(anyhow::anyhow!("Failed to receive response: {e}")),
        Err(_) => Err(anyhow::anyhow!(
            "Timeout waiting for server response for delegate {} after {} seconds.\n\
             The operation may have succeeded on the network - check server logs.\n\
             If this timeout is consistently too short, consider:\n\
             - Network latency between you and the gateway\n\
             - Delegate size and propagation time through the network",
            delegate_key.encode(),
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

    let contract = load_contract_for_publish(&config.code, params)?;

    let key = contract.key();
    // Print to stdout for scripting use (not tracing, which may be filtered)
    println!("{key}");
    Ok(())
}

pub async fn update(config: UpdateConfig, other: BaseConfig) -> anyhow::Result<()> {
    // Create ContractKey with placeholder code hash - the node will look up the actual key
    let instance_id = ContractInstanceId::try_from(config.key)?;
    let key = ContractKey::from_id_and_code(instance_id, CodeHash::new([0u8; 32]));
    println!("Updating contract {key}");
    let data = {
        let mut buf = vec![];
        File::open(&config.delta)?.read_to_end(&mut buf)?;
        wrap_update_payload(buf, config.as_state)
    };
    let request = ContractRequest::Update { key, data }.into();
    let mut client = start_api_client(other).await?;
    execute_command(request, &mut client).await?;

    // Wait for server response before closing connection (with timeout)
    tracing::info!(
        %key,
        "Request submitted, waiting for network response (timeout: {}s)...",
        RESPONSE_TIMEOUT.as_secs()
    );

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
            "Timeout waiting for server response for contract {key} after {} seconds.\n\
             The operation may have succeeded on the network - check server logs.\n\
             If this timeout is consistently too short, consider:\n\
             - Network latency between you and the gateway\n\
             - State delta size and propagation time through the network",
            RESPONSE_TIMEOUT.as_secs()
        )),
    };

    // Always gracefully close the WebSocket connection, even on timeout/error
    close_api_client(&mut client).await;

    result
}

pub async fn get(config: GetConfig, other: BaseConfig) -> anyhow::Result<()> {
    let instance_id = ContractInstanceId::try_from(config.key)?;
    // Placeholder code hash — the node resolves the contract by instance ID, not full key
    let key = ContractKey::from_id_and_code(instance_id, CodeHash::new([0u8; 32]));
    eprintln!("Getting contract {key}");
    let request = ContractRequest::Get {
        key: *key.id(),
        return_contract_code: config.return_code,
        subscribe: false,
        blocking_subscribe: false,
    }
    .into();
    let mut client = start_api_client(other).await?;
    execute_command(request, &mut client).await?;

    let result = match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            key: response_key,
            state,
            ..
        }))) => {
            let state_bytes: &[u8] = state.as_ref();
            eprintln!("Contract {response_key}: {} bytes", state_bytes.len());
            if let Some(output_path) = &config.output {
                std::fs::write(output_path, state_bytes)?;
                eprintln!("State written to {}", output_path.display());
            } else {
                use std::io::Write;
                std::io::stdout().write_all(state_bytes)?;
                std::io::stdout().flush()?;
            }
            Ok(())
        }
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::NotFound { instance_id }))) => {
            Err(anyhow::anyhow!("Contract not found: {instance_id}"))
        }
        Ok(Ok(HostResponse::ContractResponse(other))) => {
            Err(anyhow::anyhow!("Unexpected contract response: {:?}", other))
        }
        Ok(Ok(other)) => Err(anyhow::anyhow!("Unexpected response type: {:?}", other)),
        Ok(Err(e)) => Err(anyhow::anyhow!("Failed to receive response: {e}")),
        Err(_) => Err(anyhow::anyhow!(
            "Timeout waiting for get response for contract {key} after {} seconds",
            RESPONSE_TIMEOUT.as_secs()
        )),
    };

    close_api_client(&mut client).await;
    result
}

pub async fn subscribe(config: SubscribeConfig, other: BaseConfig) -> anyhow::Result<()> {
    let instance_id = ContractInstanceId::try_from(config.key)?;
    // Placeholder code hash — the node resolves the contract by instance ID, not full key
    let key = ContractKey::from_id_and_code(instance_id, CodeHash::new([0u8; 32]));
    eprintln!("Subscribing to contract {key}");
    let request = ContractRequest::Subscribe {
        key: *key.id(),
        summary: None,
    }
    .into();
    let mut client = start_api_client(other).await?;
    execute_command(request, &mut client).await?;

    // Wait for initial subscribe confirmation
    match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
            key: response_key,
            subscribed: true,
        }))) => {
            eprintln!("Subscribed to {response_key}, waiting for updates (Ctrl+C to stop)...");
        }
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
            key: response_key,
            subscribed: false,
        }))) => {
            close_api_client(&mut client).await;
            return Err(anyhow::anyhow!(
                "Subscription rejected for contract {response_key}"
            ));
        }
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::NotFound { instance_id }))) => {
            close_api_client(&mut client).await;
            return Err(anyhow::anyhow!("Contract not found: {instance_id}"));
        }
        Ok(Ok(other)) => {
            close_api_client(&mut client).await;
            return Err(anyhow::anyhow!("Unexpected response: {:?}", other));
        }
        Ok(Err(e)) => {
            close_api_client(&mut client).await;
            return Err(anyhow::anyhow!("Failed to receive response: {e}"));
        }
        Err(_) => {
            close_api_client(&mut client).await;
            return Err(anyhow::anyhow!(
                "Timeout waiting for subscribe response after {} seconds",
                RESPONSE_TIMEOUT.as_secs()
            ));
        }
    }

    // Stream update notifications until interrupted
    let mut update_count: u64 = 0;
    loop {
        tokio::select! {
            response = client.recv() => {
                match response {
                    Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                        key: update_key,
                        update,
                    })) => {
                        update_count += 1;
                        let update_bytes = extract_update_bytes(&update);
                        eprintln!(
                            "Update #{update_count} for {update_key}: {} bytes ({})",
                            update_bytes.len(),
                            describe_update_variant(&update),
                        );
                        if let Some(output_path) = &config.output {
                            atomic_write(output_path, update_bytes)?;
                            eprintln!("Update written to {}", output_path.display());
                        } else {
                            use std::io::Write;
                            std::io::stdout().write_all(update_bytes)?;
                            std::io::stdout().flush()?;
                        }
                    }
                    Ok(other) => {
                        tracing::debug!("Received non-update response: {:?}", other);
                    }
                    Err(e) => {
                        close_api_client(&mut client).await;
                        return Err(anyhow::anyhow!("Connection error: {e}"));
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nReceived {update_count} updates total");
                close_api_client(&mut client).await;
                return Ok(());
            }
        }
    }
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

/// Extract the primary state or delta bytes from an update notification.
///
/// `UpdateData` is `#[non_exhaustive]` since stdlib 0.6.0; the wildcard arm
/// returns an empty slice for unknown variants. fdev consumers display this
/// as "no payload" — they should be rebuilt against the stdlib version that
/// emits the new variant if the bytes are needed.
fn extract_update_bytes<'a>(update: &'a UpdateData<'_>) -> &'a [u8] {
    match update {
        UpdateData::State(state) => state.as_ref(),
        UpdateData::Delta(delta) => delta.as_ref(),
        UpdateData::StateAndDelta { state, .. } => state.as_ref(),
        UpdateData::RelatedState { state, .. } => state.as_ref(),
        UpdateData::RelatedDelta { delta, .. } => delta.as_ref(),
        UpdateData::RelatedStateAndDelta { state, .. } => state.as_ref(),
        _ => &[],
    }
}

/// `UpdateData` is `#[non_exhaustive]` since stdlib 0.6.0; the wildcard arm
/// returns "unknown" for future variants.
fn describe_update_variant(update: &UpdateData<'_>) -> &'static str {
    match update {
        UpdateData::State(_) => "state",
        UpdateData::Delta(_) => "delta",
        UpdateData::StateAndDelta { .. } => "state+delta",
        UpdateData::RelatedState { .. } => "related-state",
        UpdateData::RelatedDelta { .. } => "related-delta",
        UpdateData::RelatedStateAndDelta { .. } => "related-state+delta",
        _ => "unknown",
    }
}

/// Wrap raw bytes read from the user-supplied file as either a full-state
/// replacement or a delta payload. Pulled out of `update()` so the branching
/// logic is unit-testable without spinning up the async client path.
fn wrap_update_payload(buf: Vec<u8>, as_state: bool) -> UpdateData<'static> {
    if as_state {
        UpdateData::State(State::from(buf))
    } else {
        UpdateData::Delta(StateDelta::from(buf))
    }
}

/// Write to a file atomically (write to temp, then rename) to prevent partial reads.
fn atomic_write(path: &std::path::Path, data: &[u8]) -> anyhow::Result<()> {
    let dir = path.parent().unwrap_or(std::path::Path::new("."));
    let tmp = dir.join(format!(
        ".{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy()
    ));
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Minimal-but-real WASM module: just the magic + version. The
    /// helper doesn't compile the bytes; it only inspects the magic
    /// to choose the load path, so an empty module is enough.
    const TINY_WASM: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, // \0asm magic
        0x01, 0x00, 0x00, 0x00, // version 1
    ];

    fn write_tmp(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("create tmp");
        f.write_all(bytes).expect("write tmp");
        f.flush().expect("flush tmp");
        f
    }

    // ---------------------------------------------------------------
    // load_contract_for_publish — guards the #4075 fix on the publish
    // input path (raw WASM vs `fdev build`'s packaged container).
    // ---------------------------------------------------------------

    /// Raw WASM file (magic at offset 0) takes the V1-wrapping path
    /// and lands in `ContractCode.data` byte-for-byte unchanged.
    #[test]
    fn raw_wasm_loads_into_v1_container() {
        let f = write_tmp(TINY_WASM);
        let container = load_contract_for_publish(f.path(), Parameters::from(vec![]))
            .expect("raw WASM should load");
        match container {
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped)) => {
                assert_eq!(wrapped.code().data(), TINY_WASM);
            }
            other => panic!("expected V1 wasm container, got: {other:?}"),
        }
    }

    /// Packaged contract emitted by `fdev build`: 8-byte version
    /// (u64 BE) + 32-byte code hash + WASM. The bug under fix folded
    /// the 40-byte header into `ContractCode.data` and then wasmtime
    /// rejected those bytes as `compile: input bytes aren't valid utf-8`.
    /// After the fix the discrimination routes through
    /// `ContractContainer::try_from`, which strips the prefix.
    ///
    /// This test would fail on the pre-fix code path because
    /// `wrapped.code().data()` would equal the full `packed` blob
    /// (40 extra header bytes), not just `TINY_WASM`.
    #[test]
    fn packaged_contract_strips_version_prefix() {
        let mut packed = Vec::new();
        // APIVersion::Version0_0_1 encodes as u64 0 (see
        // freenet-stdlib `APIVersion::into_u64`).
        packed.extend_from_slice(&0u64.to_be_bytes());
        packed.extend_from_slice(&[0xab; 32]); // dummy code_hash
        packed.extend_from_slice(TINY_WASM);
        let f = write_tmp(&packed);

        let container = load_contract_for_publish(f.path(), Parameters::from(vec![]))
            .expect("packaged contract should load");

        match container {
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped)) => {
                assert_eq!(
                    wrapped.code().data(),
                    TINY_WASM,
                    "version prefix must be stripped before reaching ContractCode \
                     (regression: pre-fix this kept the 40-byte header attached)"
                );
            }
            other => panic!("expected V1 wasm container, got: {other:?}"),
        }
    }

    /// File whose bytes are neither WASM nor a parseable versioned
    /// container surfaces an error rather than silently producing a
    /// malformed contract that would only fail on the receiving node.
    #[test]
    fn garbage_file_surfaces_error() {
        let f = write_tmp(&[0xff; 16]);
        let err = load_contract_for_publish(f.path(), Parameters::from(vec![]))
            .expect_err("non-WASM, non-versioned bytes must surface as error");
        assert!(
            !err.to_string().is_empty(),
            "error must carry a diagnostic message, got: {err:?}"
        );
    }

    /// Empty input file: load_raw succeeds (zero bytes), magic check
    /// fails (no first 4 bytes), fallback to load_versioned tries to
    /// read a u64 version header out of nothing and returns Err.
    /// Important boundary: we MUST NOT silently produce an empty
    /// contract — that would be accepted as a valid "raw WASM of
    /// length 0" without the magic check.
    #[test]
    fn empty_file_surfaces_error() {
        let f = write_tmp(&[]);
        let err = load_contract_for_publish(f.path(), Parameters::from(vec![]))
            .expect_err("empty file must surface as error, not produce empty contract");
        assert!(
            !err.to_string().is_empty(),
            "error must carry a diagnostic message, got: {err:?}"
        );
    }

    // ---------------------------------------------------------------
    // wrap_update_payload — preserved from upstream/main; covers the
    // `--as-state` toggle on the `fdev update` path. Independent of
    // the #4075 fix above but lives in the same module.
    // ---------------------------------------------------------------

    /// `--as-state` flag toggles wrapping between `State` (full replacement,
    /// required by facade-style contracts whose `update_state` only matches
    /// `UpdateData::State`) and the default `Delta` variant.
    #[test]
    fn wrap_update_payload_state_variant() {
        let bytes = vec![1u8, 2, 3, 4];
        let wrapped = wrap_update_payload(bytes.clone(), true);
        assert!(matches!(wrapped, UpdateData::State(_)));
        assert_eq!(extract_update_bytes(&wrapped), bytes.as_slice());
        assert_eq!(describe_update_variant(&wrapped), "state");
    }

    #[test]
    fn wrap_update_payload_delta_variant_default() {
        let bytes = vec![9u8, 8, 7];
        let wrapped = wrap_update_payload(bytes.clone(), false);
        assert!(matches!(wrapped, UpdateData::Delta(_)));
        assert_eq!(extract_update_bytes(&wrapped), bytes.as_slice());
        assert_eq!(describe_update_variant(&wrapped), "delta");
    }
}
