use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use ciborium::ser::into_writer;
use clap::Subcommand;
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use freenet::server::WebApp;
use freenet_stdlib::client_api::{ContractRequest, ContractResponse, HostResponse};
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};
use xz2::write::XzEncoder;

use crate::commands::{RESPONSE_TIMEOUT, close_api_client, execute_command, start_api_client};
use crate::config::BaseConfig;

/// Pre-compiled website container contract WASM, embedded at build time.
const WEBSITE_CONTRACT_WASM: &[u8] = include_bytes!("../resources/website_contract.wasm");

/// Metadata for a website container state, serialized as CBOR.
/// Must match the struct in the website-contract crate.
#[derive(Serialize, Deserialize)]
struct WebContainerMetadata {
    version: u32,
    signature: Signature,
}

#[derive(Subcommand, Clone)]
pub enum WebsiteCommand {
    /// Generate a new signing keypair for website publishing
    Init {
        /// Output file for keys (default: ~/.config/freenet/website-keys.toml)
        #[arg(long, short)]
        output: Option<PathBuf>,
    },
    /// Publish a directory as a new website to Freenet
    Publish {
        /// Directory containing the website files (must contain index.html)
        directory: PathBuf,
        /// Key file to use (default: ~/.config/freenet/website-keys.toml)
        #[arg(long, short)]
        key_file: Option<PathBuf>,
        /// Path to a custom contract WASM file (uses built-in contract by default)
        #[arg(long)]
        contract_wasm: Option<PathBuf>,
    },
    /// Update an existing website with new content (publishes with a higher version)
    Update {
        /// Directory containing the updated website files
        directory: PathBuf,
        /// Key file to use (default: ~/.config/freenet/website-keys.toml)
        #[arg(long, short)]
        key_file: Option<PathBuf>,
        /// Path to a custom contract WASM file (uses built-in contract by default)
        #[arg(long)]
        contract_wasm: Option<PathBuf>,
    },
}

fn default_key_path() -> anyhow::Result<PathBuf> {
    let config_dir = dirs::config_dir().context("Could not determine config directory")?;
    Ok(config_dir.join("freenet").join("website-keys.toml"))
}

fn key_path(override_path: Option<&Path>) -> anyhow::Result<PathBuf> {
    match override_path {
        Some(p) => Ok(p.to_path_buf()),
        None => default_key_path(),
    }
}

fn read_signing_key(key_file: Option<&Path>) -> anyhow::Result<SigningKey> {
    let path = key_path(key_file)?;
    let config_str = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read key file: {}", path.display()))?;
    let config: toml::Table = toml::from_str(&config_str)?;
    let key_hex = config
        .get("keys")
        .and_then(|k| k.get("signing_key"))
        .and_then(|v| v.as_str())
        .context("Missing keys.signing_key in config")?;
    let key_bytes = hex::decode(key_hex).context("Invalid hex in signing key")?;
    let key_array: [u8; 32] = key_bytes
        .try_into()
        .map_err(|v: Vec<u8>| anyhow::anyhow!("Signing key must be 32 bytes, got {}", v.len()))?;
    Ok(SigningKey::from_bytes(&key_array))
}

fn compress_directory(dir: &Path) -> anyhow::Result<Vec<u8>> {
    // Verify index.html exists
    let index_path = dir.join("index.html");
    if !index_path.exists() {
        anyhow::bail!(
            "Directory {} does not contain index.html. \
             A website must have an index.html at its root.",
            dir.display()
        );
    }

    // Create tar archive
    let mut tar_buf = Vec::new();
    {
        let mut tar = tar::Builder::new(&mut tar_buf);
        tar.append_dir_all(".", dir)
            .with_context(|| format!("Failed to create tar archive from {}", dir.display()))?;
        tar.finish()?;
    }

    // Compress with xz
    let mut xz_buf = Vec::new();
    {
        let mut encoder = XzEncoder::new(&mut xz_buf, 6);
        encoder.write_all(&tar_buf)?;
        encoder.finish()?;
    }

    println!(
        "Compressed {} -> {} bytes ({} files)",
        dir.display(),
        xz_buf.len(),
        count_files(dir)?
    );
    Ok(xz_buf)
}

fn count_files(dir: &Path) -> anyhow::Result<usize> {
    let mut count = 0;
    for entry in walkdir(dir)? {
        if entry.is_file() {
            count += 1;
        }
    }
    Ok(count)
}

fn walkdir(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(walkdir(&path)?);
        } else {
            files.push(path);
        }
    }
    Ok(files)
}

fn generate_version() -> anyhow::Result<u32> {
    // Use unix timestamp / 60 as version (minutes since epoch), matching River's convention
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before Unix epoch")?
        .as_secs();
    Ok((secs / 60) as u32)
}

fn sign_webapp(
    webapp_bytes: &[u8],
    version: u32,
    signing_key: &SigningKey,
) -> WebContainerMetadata {
    let mut message = version.to_be_bytes().to_vec();
    message.extend_from_slice(webapp_bytes);
    let signature = signing_key.sign(&message);
    WebContainerMetadata { version, signature }
}

fn load_contract_wasm(custom_path: Option<&Path>) -> anyhow::Result<Vec<u8>> {
    match custom_path {
        Some(path) => {
            let mut buf = Vec::new();
            File::open(path)
                .with_context(|| format!("Failed to open contract WASM: {}", path.display()))?
                .read_to_end(&mut buf)?;
            Ok(buf)
        }
        None => Ok(WEBSITE_CONTRACT_WASM.to_vec()),
    }
}

fn build_contract_key(wasm_bytes: &[u8], verifying_key: &VerifyingKey) -> ContractKey {
    let code = ContractCode::from(wasm_bytes.to_vec());
    let params = Parameters::from(verifying_key.to_bytes().to_vec());
    let wrapped = WrappedContract::new(Arc::new(code), params);
    let api_version = ContractWasmAPIVersion::V1(wrapped);
    let container = ContractContainer::from(api_version);
    container.key()
}

pub fn init(output: Option<PathBuf>) -> anyhow::Result<()> {
    let mut key_bytes = [0u8; 32];
    rand::fill(&mut key_bytes);
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let verifying_key = signing_key.verifying_key();

    let signing_hex = hex::encode(signing_key.to_bytes());
    let verifying_hex = hex::encode(verifying_key.to_bytes());
    let config =
        format!("[keys]\nsigning_key = \"{signing_hex}\"\nverifying_key = \"{verifying_hex}\"\n");

    let path = match output {
        Some(p) => p,
        None => default_key_path()?,
    };

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    if path.exists() {
        anyhow::bail!(
            "Key file already exists at {}. \
             Remove it first if you want to generate a new keypair. \
             WARNING: Losing your signing key means you can never update your website.",
            path.display()
        );
    }

    fs::write(&path, &config)?;

    // Show the contract key so the user knows their website URL
    let wasm_bytes = WEBSITE_CONTRACT_WASM;
    let key = build_contract_key(wasm_bytes, &verifying_key);

    println!("Keypair generated and saved to: {}", path.display());
    println!();
    println!("Your website contract key: {key}");
    println!("Website URL: http://127.0.0.1:7509/v1/contract/web/{key}/");
    println!();
    println!(
        "IMPORTANT: Back up your key file! Losing it means you can never update your website."
    );
    Ok(())
}

pub async fn publish(
    directory: PathBuf,
    key_file: Option<PathBuf>,
    contract_wasm: Option<PathBuf>,
    base_config: BaseConfig,
) -> anyhow::Result<()> {
    let signing_key = read_signing_key(key_file.as_deref())?;
    let verifying_key = signing_key.verifying_key();
    let wasm_bytes = load_contract_wasm(contract_wasm.as_deref())?;

    // Compress website directory
    let webapp_bytes = compress_directory(&directory)?;

    // Sign
    let version = generate_version()?;
    let metadata = sign_webapp(&webapp_bytes, version, &signing_key);

    // Serialize metadata
    let mut metadata_bytes = Vec::new();
    into_writer(&metadata, &mut metadata_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to serialize metadata: {}", e))?;

    // Build contract
    let params = Parameters::from(verifying_key.to_bytes().to_vec());
    let code = ContractCode::from(wasm_bytes);
    let wrapped = WrappedContract::new(Arc::new(code), params);
    let api_version = ContractWasmAPIVersion::V1(wrapped);
    let contract = ContractContainer::from(api_version);
    let key = contract.key();

    // Build WebApp state
    let webapp = WebApp::from_compressed(metadata_bytes, webapp_bytes)?;
    let state: State = webapp.pack()?.into();

    println!("Publishing website as contract {key} (version {version})");

    let request = ContractRequest::Put {
        contract,
        state: state.to_vec().into(),
        related_contracts: Default::default(),
        subscribe: false,
        blocking_subscribe: false,
    }
    .into();

    let mut client = start_api_client(base_config).await?;
    execute_command(request, &mut client).await?;

    let result = match tokio::time::timeout(RESPONSE_TIMEOUT, client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse {
            key: response_key,
        }))) => {
            println!("Website published successfully!");
            println!("URL: http://127.0.0.1:7509/v1/contract/web/{response_key}/");
            Ok(())
        }
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key: response_key,
            ..
        }))) => {
            println!("Website updated successfully!");
            println!("URL: http://127.0.0.1:7509/v1/contract/web/{response_key}/");
            Ok(())
        }
        Ok(Ok(other)) => Err(anyhow::anyhow!("Unexpected response: {:?}", other)),
        Ok(Err(e)) => Err(anyhow::anyhow!("Failed to receive response: {e}")),
        Err(_) => Err(anyhow::anyhow!(
            "Timeout waiting for response after {} seconds. The operation may have succeeded.",
            RESPONSE_TIMEOUT.as_secs()
        )),
    };

    close_api_client(&mut client).await;
    result
}

pub async fn update(
    directory: PathBuf,
    key_file: Option<PathBuf>,
    contract_wasm: Option<PathBuf>,
    base_config: BaseConfig,
) -> anyhow::Result<()> {
    // Update is the same as publish -- the node handles PUT vs UPDATE based on
    // whether the contract already exists. The contract's update_state validates
    // that the version is strictly increasing.
    publish(directory, key_file, contract_wasm, base_config).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_signing_key_missing_section() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad-keys.toml");

        // Missing [keys] section entirely
        fs::write(&path, "[other]\nfoo = \"bar\"\n").unwrap();
        let result = read_signing_key(Some(path.as_path()));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing keys.signing_key"),
            "should report missing key, not panic"
        );
    }

    #[test]
    fn test_read_signing_key_missing_key_field() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad-keys.toml");

        // Has [keys] but no signing_key
        fs::write(&path, "[keys]\nverifying_key = \"abc\"\n").unwrap();
        let result = read_signing_key(Some(path.as_path()));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing keys.signing_key"),
        );
    }

    #[test]
    fn test_read_signing_key_valid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keys.toml");

        let signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let verifying_key = signing_key.verifying_key();
        let config = format!(
            "[keys]\nsigning_key = \"{}\"\nverifying_key = \"{}\"\n",
            hex::encode(signing_key.to_bytes()),
            hex::encode(verifying_key.to_bytes()),
        );
        fs::write(&path, config).unwrap();

        let result = read_signing_key(Some(path.as_path()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().to_bytes(), signing_key.to_bytes());
    }
}
