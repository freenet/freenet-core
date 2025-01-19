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

    // try to fetch the old state from the host.
    if args.put_contract {
        let ping = Ping::default();
        let serialized = serde_json::to_vec(&ping)?;
        client
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.ok_or("contract not found while putting")?,
                state: WrappedState::new(serialized),
                related_contracts: RelatedContracts::new(),
            }))
            .await?;
    } else {
        client
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
            }))
            .await?;
    }

    let mut is_subcribed = false;
    let mut local_state = loop {
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: _,
                state,
            }))) => {
                tracing::info!(key=%key, "fetched state successfully!");
                if contract_key != key {
                    return Err("unexpected key".into());
                } else {
                    let old_ping = serde_json::from_slice::<Ping>(&state)?;
                    tracing::info!(num_entries = %old_ping.len(), "old state fetched successfully!");

                    // the contract already put, so we subscribe to the contract.
                    client
                        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                            key: contract_key,
                            summary: None,
                        }))
                        .await?;

                    break old_ping;
                }
            }
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                tracing::info!(key=%key, "put ping contract successfully!");
                // we successfully put the contract, so we subscribe to the contract.
                if key == contract_key {
                    if let Err(e) = client
                        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                            key,
                            summary: None,
                        }))
                        .await
                    {
                        tracing::error!(err=%e);
                        return Err(e.into());
                    }
                } else {
                    return Err("unexpected key".into());
                }
                break Ping::default();
            }
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
                ..
            }))) => {
                if key == contract_key {
                    tracing::debug!(key=%key, "Marking as subscribed");
                    is_subcribed = true;
                } else {
                    return Err("unexpected key".into());
                }
                if subscribed {
                    tracing::info!(key=%key, "subscribed successfully!");
                } else {
                    tracing::error!(key=%key, "failed to subscribe");
                    return Err("failed to subscribe".into());
                }
                tracing::info!("subscribed successfully!");
            }
            Ok(Ok(other)) => {
                tracing::warn!("unexpected response: {}", other);
            }
            Ok(Err(err)) => {
                tracing::error!(err=%err);
                return Err(err.into());
            }
            Err(_) => {
                tracing::error!("failed to fetch state, timeout");
                return Err("failed to fetch state".into());
            }
        }
    };

    let mut send_tick = tokio::time::interval(args.parameters.frequency);

    let mut errors = 0;
    loop {
        if errors > 100 {
            tracing::error!("too many errors, shutting down...");
            return Err("too many errors".into());
        }
        tokio::select! {
            _ = send_tick.tick() => {
                if is_subcribed {
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
                                ContractResponse::PutResponse { key } => {
                                    tracing::info!(key=%key, "put ping contract successfully!");
                                    // we successfully put the contract, so we subscribe to the contract.
                                    if key != contract_key {
                                        return Err("unexpected key".into());
                                    }
                                    client.send(ClientRequest::ContractOp(ContractRequest::Subscribe { key, summary: None })).await.inspect_err(|e| {
                                        tracing::error!(err=%e);
                                    })?;
                                },
                                ContractResponse::SubscribeResponse { key, .. } => {
                                    if key != contract_key {
                                        return Err("unexpected key".into());
                                    }
                                    tracing::debug!(key=%key, "Marking as subscribed");
                                    is_subcribed = true;
                                },
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
                                _ => {},
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
