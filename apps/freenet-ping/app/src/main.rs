use std::time::Duration;

use clap::Parser;
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::{
        ContractContainer, Parameters, RelatedContracts, StateDelta, UpdateData, WrappedState,
    },
};
use names::Generator;
use tokio::time::timeout;

#[derive(clap::Parser)]
struct Args {
    #[clap(long, default_value = "localhost:50509")]
    host: String,
    #[clap(long, default_value = "info")]
    log_level: tracing::level_filters::LevelFilter,
    #[clap(flatten)]
    parameters: PingContractOptions,
}

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

    const PING_CODE: &[u8] =
        include_bytes!("../../contracts/ping/build/freenet/freenet_ping_contract");

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
    let container = ContractContainer::try_from((PING_CODE.to_vec(), &params))?;
    let contract_key = container.key();

    // try to fetch the old state from the host.
    client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key: contract_key,
            return_contract_code: false,
        }))
        .await?;

    let resp = timeout(Duration::from_secs(5), client.recv()).await;

    let mut is_subcribed = false;
    let mut local_state = match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            key,
            contract: _,
            state,
        }))) => {
            tracing::info!(key=%key, "fetched state successfully!");
            if contract_key != key || (*state).is_empty() {
                client
                    .send(ClientRequest::ContractOp(ContractRequest::Put {
                        contract: container,
                        state: WrappedState::new(vec![]),
                        related_contracts: RelatedContracts::new(),
                    }))
                    .await?;
                Ping::default()
            } else {
                let old_ping = serde_json::from_slice::<Ping>(&state)?;
                let mut ping = Ping::default();
                ping.merge(old_ping, args.parameters.ttl);

                for name in ping.keys() {
                    tracing::info!("Hello, {}!", name);
                }

                // the contract already put, so we subscribe to the contract.
                client
                    .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                        key: contract_key,
                        summary: None,
                    }))
                    .await?;

                ping
            }
        }
        _ => {
            tracing::info!("failed to fetch state, putting a new contract...");
            client
                .send(ClientRequest::ContractOp(ContractRequest::Put {
                    contract: container,
                    state: WrappedState::new(vec![]),
                    related_contracts: RelatedContracts::new(),
                }))
                .await?;
            Ping::default()
        }
    };

    let mut send_tick = tokio::time::interval(args.parameters.frequency);

    let mut generator = Generator::default();
    let mut errors = 0;
    loop {
        if errors > 100 {
            tracing::error!("too many errors, shutting down...");
            return Err("too many errors".into());
        }
        tokio::select! {
            _ = send_tick.tick() => {
                if is_subcribed {
                    let name = generator.next().unwrap();
                    let mut ping = Ping::default();
                    ping.insert(name.clone());
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
                                    if key == contract_key {
                                        if let Err(e) = client.send(ClientRequest::ContractOp(ContractRequest::Subscribe { key, summary: None })).await {
                                            tracing::error!(err=%e);
                                            return Err(e.into());
                                        }
                                    }
                                },
                                ContractResponse::SubscribeResponse { key, .. } => {
                                    tracing::debug!(key=%key, "Received subscribe response");
                                    if key == contract_key {
                                        tracing::debug!(key=%key, "Marking as subscribed");
                                        is_subcribed = true;
                                    }
                                },
                                ContractResponse::UpdateNotification { key, update } => {
                                    if key == contract_key {
                                        let mut handle_update = |state: &[u8]| {
                                            let ping = if state.is_empty() {
                                                Ping::default()
                                            } else {
                                                match serde_json::from_slice::<Ping>(state) {
                                                    Ok(p) => p,
                                                    Err(e) => return Err(e),
                                                }
                                            };

                                            for (name, created) in ping.iter() {
                                                if !local_state.contains_key(name) {
                                                    tracing::info!("Hello, {}! @ ({created})", name);
                                                }
                                            }

                                            local_state.merge(ping, args.parameters.ttl);
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
