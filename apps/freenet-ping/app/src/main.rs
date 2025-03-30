use std::{path::PathBuf, time::Duration};

use clap::Parser;
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use tokio::time::timeout;

#[derive(clap::Parser)]
struct Args {
    #[clap(long, default_value = "localhost:50509")]
    host: String,
    #[clap(long, default_value = "info")]
    log_level: tracing::level_filters::LevelFilter,
    #[clap(flatten)]
    parameters: PingContractOptions,
    #[clap(long)]
    put_contract: bool,
}

const PACKAGE_DIR: &str = env!("CARGO_MANIFEST_DIR");
const PATH_TO_CONTRACT: &str = "../contracts/ping/build/freenet/freenet_ping_contract";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_ansi(true)
        .with_level(true)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_max_level(args.log_level)
        .with_line_number(true)
        .init();

    // create a websocket connection to host.
    let uri = format!(
        "ws://{}/v1/contract/command?encodingProtocol=native",
        args.host
    );
    let (stream, _resp) = tokio_tungstenite::connect_async(&uri).await.map_err(|e| {
        tracing::error!(err=%e);
        e
    })?;
    let mut client = WebApi::start(stream);

    let params = Parameters::from(serde_json::to_vec(&args.parameters).unwrap());
    let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
    tracing::info!(path=%path_to_code.display(), "loading contract code");
    let code = std::fs::read(path_to_code).ok();

    let container = code
        .map(|bytes| ContractContainer::try_from((bytes, &params)))
        .transpose()?;
    let contract_key = ContractKey::from_params(args.parameters.code_key, params.clone())?;

    // Step 1: Put the contract or get it, and wait for response
    let mut is_subscribed = false;
    let mut local_state: Ping;
    
    if args.put_contract {
        // Put contract and wait for response
        let ping = Ping::default();
        let serialized = serde_json::to_vec(&ping)?;
        client
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.ok_or("contract not found while putting")?,
                state: WrappedState::new(serialized),
                related_contracts: RelatedContracts::new(),
                subscribe: false,
            }))
            .await?;
        
        // Wait for put response
        let key = wait_for_put_response(&mut client, &contract_key).await?;
        tracing::info!(key=%key, "put ping contract successfully!");
        local_state = Ping::default();
    } else {
        // Get contract and wait for response
        client
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;
        
        // Wait for get response
        local_state = wait_for_get_response(&mut client, &contract_key).await?;
    }
    
    // Step 2: Subscribe to the contract and wait for subscription confirmation
    tracing::info!("Subscribing to contract...");
    client
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key: contract_key,
            summary: None,
        }))
        .await?;
    
    // Wait for subscription response
    wait_for_subscribe_response(&mut client, &contract_key).await?;
    is_subscribed = true;
    tracing::info!(key=%contract_key, "subscribed successfully!");

    // Start the ping timer only after subscription confirmed
    let mut send_tick = tokio::time::interval(args.parameters.frequency);

    let mut errors = 0;
    loop {
        if errors > 100 {
            tracing::error!("too many errors, shutting down...");
            return Err("too many errors".into());
        }
        tokio::select! {
            _ = send_tick.tick() => {
                if is_subscribed {
                    let mut ping = Ping::default();
                    ping.insert(args.parameters.tag.clone());
                    if let Err(e) = client.send(ClientRequest::ContractOp(ContractRequest::Update {
                        key: contract_key,
                        data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&ping).unwrap())),
                    })).await {
                        tracing::error!(err=%e, "failed to send update request");
                    }
                }
            },
            res = client.recv() => {
                match res {
                    Ok(resp) => match resp {
                        HostResponse::ContractResponse(resp) => {
                            match resp {
                                ContractResponse::UpdateNotification { key, update } => {
                                    if key != contract_key {
                                        return Err("unexpected key".into());
                                    }
                                    let mut handle_update = |state: &[u8]| {
                                        let new_ping = if state.is_empty() {
                                            Ping::default()
                                        } else {
                                            match serde_json::from_slice::<Ping>(state) {
                                                Ok(p) => p,
                                                Err(e) => return Err(e),
                                            }
                                        };

                                        let updates = local_state.merge(new_ping, args.parameters.ttl);

                                        for (name, update) in updates.into_iter() {
                                            tracing::info!("{} last updated at {}", name, update);
                                        }
                                        Ok(())
                                    };

                                    match update {
                                        UpdateData::State(state) =>  {
                                            if let Err(e) = handle_update(&state) {
                                                tracing::error!(err=%e);
                                            }
                                        },
                                        UpdateData::Delta(delta) => {
                                            if let Err(e) = handle_update(&delta) {
                                                tracing::error!(err=%e);
                                            }
                                        },
                                        UpdateData::StateAndDelta { state, delta } => {
                                            if let Err(e) = handle_update(&state) {
                                                tracing::error!(err=%e);
                                            }

                                            if let Err(e) = handle_update(&delta) {
                                                tracing::error!(err=%e);
                                            }
                                        },
                                        _ => unreachable!("unknown state"),
                                    }
                                },
                                _ => {
                                    tracing::debug!("Received other contract response: {:?}", resp);
                                },
                            }
                        },
                        HostResponse::DelegateResponse { .. } => {},
                        HostResponse::Ok => {},
                        _ => unreachable!(),
                    },
                    Err(e) => {
                        tracing::error!(err=%e);
                        errors += 1;
                    },
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down...");
                break;
            }
        }
    }
    Ok(())
}

async fn wait_for_put_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<ContractKey, Box<dyn std::error::Error + Send + Sync + 'static>> {
    loop {
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                if &key == expected_key {
                    return Ok(key);
                } else {
                    return Err("unexpected key".into());
                }
            }
            Ok(Ok(other)) => {
                tracing::warn!("Unexpected response while waiting for put: {}", other);
            }
            Ok(Err(err)) => {
                tracing::error!(err=%err);
                return Err(err.into());
            }
            Err(_) => {
                return Err("timeout waiting for put response".into());
            }
        }
    }
}

async fn wait_for_get_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<Ping, Box<dyn std::error::Error + Send + Sync + 'static>> {
    loop {
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: _,
                state,
            }))) => {
                if &key != expected_key {
                    return Err("unexpected key".into());
                }
                
                let old_ping = serde_json::from_slice::<Ping>(&state)?;
                tracing::info!(num_entries = %old_ping.len(), "old state fetched successfully!");
                return Ok(old_ping);
            }
            Ok(Ok(other)) => {
                tracing::warn!("Unexpected response while waiting for get: {}", other);
            }
            Ok(Err(err)) => {
                tracing::error!(err=%err);
                return Err(err.into());
            }
            Err(_) => {
                return Err("timeout waiting for get response".into());
            }
        }
    }
}

async fn wait_for_subscribe_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    loop {
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
                ..
            }))) => {
                if &key != expected_key {
                    return Err("unexpected key".into());
                }
                
                if subscribed {
                    return Ok(());
                } else {
                    return Err("failed to subscribe".into());
                }
            }
            Ok(Ok(other)) => {
                tracing::warn!("Unexpected response while waiting for subscribe: {}", other);
            }
            Ok(Err(err)) => {
                tracing::error!(err=%err);
                return Err(err.into());
            }
            Err(_) => {
                return Err("timeout waiting for subscribe response".into());
            }
        }
    }
}
