use std::io::{Cursor, Read};

use byteorder::{BigEndian, ReadBytesExt};
use ciborium::{de::from_reader, ser::into_writer};
use ed25519_dalek::{Signature, VerifyingKey};
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

const MAX_METADATA_SIZE: u64 = 1024;
const MAX_WEB_SIZE: u64 = 100 * 1024 * 1024;

/// Metadata for a website container state, serialized as CBOR.
#[derive(Serialize, Deserialize)]
pub struct WebContainerMetadata {
    pub version: u32,
    pub signature: Signature,
}

pub struct WebsiteContainerContract;

/// Parse version from a state byte slice, with bounds checking on metadata size.
fn parse_version(state_bytes: &[u8]) -> Result<u32, ContractError> {
    let mut cursor = Cursor::new(state_bytes);
    let metadata_size = cursor
        .read_u64::<BigEndian>()
        .map_err(|e| ContractError::Other(format!("Failed to read metadata size: {}", e)))?;
    if metadata_size > MAX_METADATA_SIZE {
        return Err(ContractError::Other(format!(
            "Metadata size {} exceeds maximum allowed size of {} bytes",
            metadata_size, MAX_METADATA_SIZE
        )));
    }
    let mut metadata_bytes = vec![0; metadata_size as usize];
    cursor
        .read_exact(&mut metadata_bytes)
        .map_err(|e| ContractError::Other(format!("Failed to read metadata: {}", e)))?;
    let metadata: WebContainerMetadata =
        from_reader(&metadata_bytes[..]).map_err(|e| ContractError::Deser(e.to_string()))?;
    Ok(metadata.version)
}

#[contract]
impl ContractInterface for WebsiteContainerContract {
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        #[cfg(all(not(test), target_arch = "wasm32", feature = "trace"))]
        {
            freenet_stdlib::log::info("Starting validate_state");
            freenet_stdlib::log::info(&format!("Parameters length: {}", parameters.as_ref().len()));
            freenet_stdlib::log::info(&format!("State length: {}", state.as_ref().len()));
        }

        // Extract and deserialize verifying key from parameters
        let params_bytes: &[u8] = parameters.as_ref();
        if params_bytes.len() != 32 {
            return Err(ContractError::Other(
                "Parameters must be 32 bytes".to_string(),
            ));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(params_bytes);

        let verifying_key = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|e| ContractError::Other(format!("Invalid public key: {}", e)))?;

        // Parse WebApp format: [metadata_length: u64][metadata: bytes][web_length: u64][web: bytes]
        let mut cursor = Cursor::new(state.as_ref());

        let metadata_size = cursor
            .read_u64::<BigEndian>()
            .map_err(|e| ContractError::Other(format!("Failed to read metadata size: {}", e)))?;

        if metadata_size > MAX_METADATA_SIZE {
            return Err(ContractError::Other(format!(
                "Metadata size {} exceeds maximum allowed size of {} bytes",
                metadata_size, MAX_METADATA_SIZE
            )));
        }

        let mut metadata_bytes = vec![0; metadata_size as usize];
        cursor
            .read_exact(&mut metadata_bytes)
            .map_err(|e| ContractError::Other(format!("Failed to read metadata: {}", e)))?;

        let metadata: WebContainerMetadata =
            match from_reader::<WebContainerMetadata, _>(&metadata_bytes[..]) {
                Ok(m) => {
                    #[cfg(all(not(test), target_arch = "wasm32", feature = "trace"))]
                    freenet_stdlib::log::info(&format!(
                        "Successfully parsed metadata with version {}",
                        m.version
                    ));
                    m
                }
                Err(e) => {
                    #[cfg(all(not(test), target_arch = "wasm32", feature = "trace"))]
                    freenet_stdlib::log::info(&format!("CBOR parsing error: {}", e));
                    return Err(ContractError::Deser(e.to_string()));
                }
            };

        if metadata.version == 0 {
            return Err(ContractError::InvalidState);
        }

        let web_size = cursor
            .read_u64::<BigEndian>()
            .map_err(|e| ContractError::Other(format!("Failed to read web size: {}", e)))?;

        if web_size > MAX_WEB_SIZE {
            return Err(ContractError::Other(format!(
                "Web size {} exceeds maximum allowed size of {} bytes",
                web_size, MAX_WEB_SIZE
            )));
        }

        let mut webapp_bytes = vec![0; web_size as usize];
        cursor
            .read_exact(&mut webapp_bytes)
            .map_err(|e| ContractError::Other(format!("Failed to read web bytes: {}", e)))?;

        // Verify signature over version_bytes || webapp_bytes
        let mut message = metadata.version.to_be_bytes().to_vec();
        message.extend_from_slice(&webapp_bytes);

        #[cfg(all(not(test), target_arch = "wasm32", feature = "trace"))]
        {
            freenet_stdlib::log::info(&format!(
                "Verifying signature: {} bytes message, version {}",
                message.len(),
                metadata.version
            ));
        }

        let verify_result = verifying_key.verify_strict(&message, &metadata.signature);

        if let Err(e) = verify_result {
            #[cfg(all(not(test), target_arch = "wasm32", feature = "trace"))]
            freenet_stdlib::log::info(&format!("Signature verification failed: {}", e));
            return Err(ContractError::Other(format!(
                "Signature verification failed: {}",
                e
            )));
        }

        #[cfg(all(not(test), target_arch = "wasm32", feature = "trace"))]
        freenet_stdlib::log::info("Validation successful");

        Ok(ValidateResult::Valid)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        #[cfg(all(not(test), target_arch = "wasm32", feature = "trace"))]
        {
            freenet_stdlib::log::info("Starting update_state");
            freenet_stdlib::log::info(&format!("Current state length: {}", state.as_ref().len()));
            freenet_stdlib::log::info(&format!("Update data count: {}", data.len()));
        }

        let current_version = if state.as_ref().is_empty() {
            0
        } else {
            parse_version(state.as_ref())?
        };

        if let Some(UpdateData::State(new_state)) = data.into_iter().next() {
            let new_version = parse_version(new_state.as_ref())?;

            if new_version <= current_version {
                return Err(ContractError::InvalidUpdateWithInfo {
                    reason: format!(
                        "New state version {} must be higher than current version {}",
                        new_version, current_version
                    ),
                });
            }

            #[cfg(all(not(test), target_arch = "wasm32", feature = "trace"))]
            freenet_stdlib::log::info(&format!(
                "Update successful: version {} -> {}",
                current_version, new_version
            ));

            Ok(UpdateModification::valid(new_state))
        } else {
            #[cfg(all(not(test), target_arch = "wasm32", feature = "trace"))]
            freenet_stdlib::log::info("Update failed: no valid update data provided");
            Err(ContractError::InvalidUpdate)
        }
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        if state.as_ref().is_empty() {
            return Ok(StateSummary::from(Vec::new()));
        }

        let version = parse_version(state.as_ref())?;
        let mut summary = Vec::new();
        into_writer(&version, &mut summary).map_err(|e| ContractError::Deser(e.to_string()))?;

        Ok(StateSummary::from(summary))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        if state.as_ref().is_empty() {
            return Ok(StateDelta::from(Vec::new()));
        }

        let current_version = parse_version(state.as_ref())?;
        let summary_version: u32 =
            from_reader(summary.as_ref()).map_err(|e| ContractError::Deser(e.to_string()))?;

        if current_version > summary_version {
            Ok(StateDelta::from(state.as_ref().to_vec()))
        } else {
            Ok(StateDelta::from(Vec::new()))
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    fn create_test_keypair() -> (SigningKey, VerifyingKey) {
        let signing_key = SigningKey::from_bytes(&SigningKey::generate(&mut OsRng).to_bytes());
        let verifying_key = signing_key.verifying_key();
        (signing_key, verifying_key)
    }

    fn create_test_state(
        version: u32,
        compressed_webapp: &[u8],
        signing_key: &SigningKey,
    ) -> Vec<u8> {
        let mut message = version.to_be_bytes().to_vec();
        message.extend_from_slice(compressed_webapp);
        let signature = signing_key.sign(&message);

        let metadata = WebContainerMetadata { version, signature };
        let mut metadata_bytes = Vec::new();
        into_writer(&metadata, &mut metadata_bytes).unwrap();

        let mut state = Vec::new();
        state.extend_from_slice(&(metadata_bytes.len() as u64).to_be_bytes());
        state.extend_from_slice(&metadata_bytes);
        state.extend_from_slice(&(compressed_webapp.len() as u64).to_be_bytes());
        state.extend_from_slice(compressed_webapp);
        state
    }

    #[test]
    fn test_empty_state_fails_validation() {
        let result = WebsiteContainerContract::validate_state(
            Parameters::from(vec![]),
            State::from(vec![]),
            RelatedContracts::default(),
        );
        assert!(matches!(result, Err(ContractError::Other(_))));
    }

    #[test]
    fn test_valid_state() {
        let (signing_key, verifying_key) = create_test_keypair();
        let compressed_webapp = b"Hello, World!";
        let state = create_test_state(1, compressed_webapp, &signing_key);

        let result = WebsiteContainerContract::validate_state(
            Parameters::from(verifying_key.to_bytes().to_vec()),
            State::from(state),
            RelatedContracts::default(),
        );
        assert!(matches!(result, Ok(ValidateResult::Valid)));
    }

    #[test]
    fn test_invalid_version() {
        let (signing_key, verifying_key) = create_test_keypair();
        let compressed_webapp = b"Hello, World!";
        let state = create_test_state(0, compressed_webapp, &signing_key);

        let result = WebsiteContainerContract::validate_state(
            Parameters::from(verifying_key.to_bytes().to_vec()),
            State::from(state),
            RelatedContracts::default(),
        );
        assert!(matches!(result, Err(ContractError::InvalidState)));
    }

    #[test]
    fn test_invalid_signature() {
        let (_, verifying_key) = create_test_keypair();
        let (wrong_signing_key, _) = create_test_keypair();
        let compressed_webapp = b"Hello, World!";
        let state = create_test_state(1, compressed_webapp, &wrong_signing_key);

        let result = WebsiteContainerContract::validate_state(
            Parameters::from(verifying_key.to_bytes().to_vec()),
            State::from(state),
            RelatedContracts::default(),
        );
        assert!(matches!(result, Err(ContractError::Other(_))));
    }

    #[test]
    fn test_update_state_version_check() {
        let (signing_key, _) = create_test_keypair();

        let current_state = create_test_state(1, b"Original", &signing_key);

        // Same version should fail
        let new_state = create_test_state(1, b"New Content", &signing_key);
        let result = WebsiteContainerContract::update_state(
            Parameters::from(vec![]),
            State::from(current_state.clone()),
            vec![UpdateData::State(State::from(new_state))],
        );
        assert!(matches!(
            result,
            Err(ContractError::InvalidUpdateWithInfo { reason: _ })
        ));

        // Higher version should succeed
        let new_state = create_test_state(2, b"New Content", &signing_key);
        let result = WebsiteContainerContract::update_state(
            Parameters::from(vec![]),
            State::from(current_state),
            vec![UpdateData::State(State::from(new_state))],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_summarize_and_delta() {
        let (signing_key, _) = create_test_keypair();
        let state = create_test_state(2, b"Content", &signing_key);

        let summary = WebsiteContainerContract::summarize_state(
            Parameters::from(vec![]),
            State::from(state.clone()),
        )
        .unwrap();

        let summary_version: u32 = from_reader(summary.as_ref()).unwrap();
        assert_eq!(summary_version, 2);

        // Delta with older summary should return full state
        let mut old_summary = Vec::new();
        into_writer(&1u32, &mut old_summary).unwrap();
        let delta = WebsiteContainerContract::get_state_delta(
            Parameters::from(vec![]),
            State::from(state.clone()),
            StateSummary::from(old_summary),
        )
        .unwrap();
        assert!(!delta.as_ref().is_empty());

        // Delta with same version should return empty
        let mut same_summary = Vec::new();
        into_writer(&2u32, &mut same_summary).unwrap();
        let delta = WebsiteContainerContract::get_state_delta(
            Parameters::from(vec![]),
            State::from(state),
            StateSummary::from(same_summary),
        )
        .unwrap();
        assert!(delta.as_ref().is_empty());
    }

    #[test]
    fn test_update_state_empty_data() {
        let (signing_key, _) = create_test_keypair();
        let current_state = create_test_state(1, b"Content", &signing_key);

        let result = WebsiteContainerContract::update_state(
            Parameters::from(vec![]),
            State::from(current_state),
            vec![],
        );
        assert!(matches!(result, Err(ContractError::InvalidUpdate)));
    }

    #[test]
    fn test_update_state_delta_instead_of_state() {
        let (signing_key, _) = create_test_keypair();
        let current_state = create_test_state(1, b"Content", &signing_key);

        let result = WebsiteContainerContract::update_state(
            Parameters::from(vec![]),
            State::from(current_state),
            vec![UpdateData::Delta(StateDelta::from(vec![1, 2, 3]))],
        );
        assert!(matches!(result, Err(ContractError::InvalidUpdate)));
    }

    #[test]
    fn test_oversized_metadata_rejected() {
        // Craft a state with metadata_size > MAX_METADATA_SIZE (1024)
        let oversized_metadata_len: u64 = 2048;
        let mut state = Vec::new();
        state.extend_from_slice(&oversized_metadata_len.to_be_bytes());
        state.extend_from_slice(&vec![0u8; oversized_metadata_len as usize]);
        // Pad with web_size + web content
        state.extend_from_slice(&0u64.to_be_bytes());

        // validate_state should reject oversized metadata
        let result = WebsiteContainerContract::validate_state(
            Parameters::from(vec![0u8; 32]),
            State::from(state.clone()),
            RelatedContracts::default(),
        );
        assert!(
            matches!(result, Err(ContractError::Other(ref msg)) if msg.contains("exceeds maximum")),
            "validate_state should reject oversized metadata"
        );

        // update_state should also reject oversized metadata in new state
        let (signing_key, _) = create_test_keypair();
        let current_state = create_test_state(1, b"Content", &signing_key);
        let result = WebsiteContainerContract::update_state(
            Parameters::from(vec![]),
            State::from(current_state),
            vec![UpdateData::State(State::from(state))],
        );
        assert!(
            matches!(result, Err(ContractError::Other(ref msg)) if msg.contains("exceeds maximum")),
            "update_state should reject oversized metadata in new state"
        );
    }
}
