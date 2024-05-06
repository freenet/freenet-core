use std::{collections::HashSet, time::Duration};

use clap::Parser;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::{
        ContractContainer, ContractKey, Parameters, RelatedContracts, StateDelta, UpdateData,
        WrappedState,
    },
};
use names::Generator;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
struct Ping {
    from: HashSet<String>,
}

#[derive(clap::Parser)]
struct Args {
    #[clap(long, default_value = "ws://localhost:50509")]
    host: String,
    #[clap(long, default_value = "info")]
    log_level: tracing::level_filters::LevelFilter,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_ansi(true)
        .with_level(true)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_max_level(args.log_level)
        .init();

    const PING_CODE: &[u8] =
        include_bytes!("../../contracts/ping/build/freenet/freenet_ping_contract");

    // create a websocket connection to host.
    let (stream, _resp) = tokio_tungstenite::connect_async(args.host).await.map_err(|e| {
        tracing::error!(err=%e);
        e
    })?;
    let mut client = WebApi::start(stream);
    // put contract first
    let params = Parameters::from(vec![]);
    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: ContractContainer::try_from((PING_CODE.to_vec(), &params))?,
            state: WrappedState::new(vec![]),
            related_contracts: RelatedContracts::new(),
        }))
        .await?;

    let mut send_tick = tokio::time::interval(Duration::from_secs(1));
    let mut fetch_tick = tokio::time::interval(Duration::from_secs_f64(1.5));
    let mut contract_key: Option<ContractKey> = None;

    let mut local_state = Ping::default();

    let mut generator = Generator::default();
    loop {
        tokio::select! {
          _ = send_tick.tick() => {
            let name = generator.next().unwrap();

            local_state.from.insert(name.clone());
            if let Some(contract_key) = contract_key.as_ref() {
              if let Err(e) = client.send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key.clone(),
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&HashSet::from([name])).unwrap())),
              })).await {
                tracing::error!(%e);
              }
            }
          },
          _ = fetch_tick.tick() => {
            if let Some(contract_key) = contract_key.as_ref() {
              if let Err(e) = client.send(ClientRequest::ContractOp(ContractRequest::Get { key: contract_key.clone(), fetch_contract: false })).await {
                tracing::error!(%e);
              }
            }
          },
          res = client.recv() => {
            match res {
              Ok(resp) => match resp {
                HostResponse::ContractResponse(resp) => {
                  match resp {
                    ContractResponse::GetResponse { key, contract: _, state } => {
                      match contract_key.as_ref() {
                        Some(k) if key.eq(k) => {
                          let ping = match serde_json::from_slice::<Ping>(&state) {
                            Ok(p) => p,
                            Err(e) => {
                              tracing::error!(%e);
                              continue;
                            },
                          };

                          for name in local_state.from.difference(&ping.from) {
                            tracing::info!("Hello {}!", name);
                          }
                        },
                        _ => continue,
                      }
                    },
                    ContractResponse::PutResponse { key } => {
                      contract_key = Some(key);
                    },
                    ContractResponse::UpdateNotification { .. } => {},
                    ContractResponse::UpdateResponse { summary, key } => {
                      match contract_key.as_ref() {
                        Some(k) if key.eq(k) => {
                          let ping = match serde_json::from_slice::<Ping>(&summary) {
                            Ok(p) => p,
                            Err(e) => {
                              tracing::error!(%e);
                              continue;
                            },
                          };

                          for name in local_state.from.difference(&ping.from) {
                            tracing::info!("Hello {}!", name);
                          }
                          local_state.from.extend(ping.from);
                        }
                        _ => continue,
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
                tracing::error!(%e);
              },
            }
          }
          _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutting down...");
            break;
          }
        }
    }
    Ok(())
}

