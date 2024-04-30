use std::time::Duration;

use clap::Parser;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::{
        ContractContainer, ContractKey, Parameters, RelatedContracts, StateDelta, UpdateData, WrappedState
    },
};

#[derive(clap::Parser)]
struct Args {
    #[clap(short, long, default_value = "http://localhost:8080")]
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
    let (stream, _resp) = tokio_tungstenite::connect_async(args.host).await?;
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

    loop {
        tokio::select! {
          _ = send_tick.tick() => {
            if let Some(contract_key) = contract_key.as_ref() {
              if let Err(e) = client.send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key.clone(),
                data: UpdateData::Delta(StateDelta::from([0, 0, 0].as_slice())),
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
                          tracing::info!(state = ?state);
                        },
                        _ => continue,
                      }
                    },
                    ContractResponse::PutResponse { key } => {
                      contract_key = Some(key);
                    },
                    ContractResponse::UpdateNotification { .. } => {},
                    ContractResponse::UpdateResponse { .. } => {},
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
