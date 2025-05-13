use std::time::Duration;

use anyhow::{anyhow, Result};
use freenet_stdlib::{client_api::WebApi, prelude::*};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tracing::info;

pub const PACKAGE_DIR: &str = env!("CARGO_MANIFEST_DIR");
pub const PATH_TO_CONTRACT: &str =
    "../target/wasm32-unknown-unknown/release/freenet_ping_contract.wasm";
pub const APP_TAG: &str = "ping-test";

pub const MAX_UPDATE_RETRIES: u32 = 5;
pub const INITIAL_DELAY_MS: u64 = 100;
pub const MAX_DELAY_MS: u64 = 5000;
pub const MAX_PROPAGATION_CHECKS: u32 = 10;
pub const PROPAGATION_CHECK_INTERVAL_MS: u64 = 1000;

pub async fn connect_with_retry(uri: &str, max_retries: u32) -> Result<WebApi> {
    let mut retry_count = 0;
    let mut delay_ms = INITIAL_DELAY_MS;

    loop {
        match connect_async(uri).await {
            Ok((stream, _)) => {
                return Ok(WebApi::start(stream));
            }
            Err(e) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    return Err(anyhow!(
                        "Failed to connect after {} retries: {}",
                        max_retries,
                        e
                    ));
                }

                info!(
                    "Connection attempt {} failed: {}. Retrying in {}ms...",
                    retry_count, e, delay_ms
                );
                sleep(Duration::from_millis(delay_ms)).await;
                delay_ms = std::cmp::min(delay_ms * 2, MAX_DELAY_MS);
            }
        }
    }
}
