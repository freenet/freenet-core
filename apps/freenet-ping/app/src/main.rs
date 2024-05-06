use std::{collections::HashSet, time::Duration};

use clap::Parser;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::{
        ContractContainer, Parameters, RelatedContracts, StateDelta, UpdateData,
        WrappedState,
    },
};
use names::Generator;
use rand::RngCore;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct Ping {
    from: HashSet<String>,
}

#[derive(clap::Parser)]
struct Args {
    #[clap(long, default_value = "localhost:50509")]
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
        .with_line_number(true)
        .init();

    const PING_CODE: &[u8] =
        include_bytes!("../../contracts/ping/build/freenet/freenet_ping_contract");

    // create a websocket connection to host.
    let uri = format!(
        "ws://{}/contract/command?encodingProtocol=native",
        args.host
    );
    let (stream, _resp) = tokio_tungstenite::connect_async(&uri).await.map_err(|e| {
        tracing::error!(err=%e);
        e
    })?;
    let mut client = WebApi::start(stream);
    // put contract first
    // Generate a random parameters so that we can add multiple ping contracts to the host.
    let params = {
      let mut data = [0u8; 32];
      rand::thread_rng().fill_bytes(&mut data);
      Parameters::from(data.to_vec())
    };
    let container = ContractContainer::try_from((PING_CODE.to_vec(), &params))?;
    let contract_key = container.key();
    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: container,
            // state: WrappedState::new(serde_json::to_vec(&Ping::default()).unwrap()),
            state: WrappedState::new(vec![]),
            related_contracts: RelatedContracts::new(),
        }))
        .await?;

    let mut send_tick = tokio::time::interval(Duration::from_secs(1));
    let mut fetch_tick = tokio::time::interval(Duration::from_secs_f64(1.5));
    let mut local_state = Ping::default();

    let mut generator = Generator::default();
    loop {
        tokio::select! {
          _ = send_tick.tick() => {
            let name = generator.next().unwrap();

            local_state.from.insert(name.clone());
            let mut ping = Ping::default();
            ping.from.insert(name.clone());
            if let Err(e) = client.send(ClientRequest::ContractOp(ContractRequest::Update {
              key: contract_key.clone(),
              data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&ping).unwrap())),
            })).await {
              tracing::error!(err=%e, "failed to send update request");
            }
          },
          _ = fetch_tick.tick() => {
            if let Err(e) = client.send(ClientRequest::ContractOp(ContractRequest::Get { key: contract_key.clone(), fetch_contract: false })).await {
              tracing::error!(err=%e);
            }
          },
          res = client.recv() => {
            match res {
              Ok(resp) => match resp {
                HostResponse::ContractResponse(resp) => {
                  match resp {
                    ContractResponse::GetResponse { key, contract: _, state } => {
                      if contract_key.eq(&key) {
                        let ping = if state.is_empty() {
                          Ping::default()
                        } else {
                          match serde_json::from_slice::<Ping>(&state) {
                            Ok(p) => p,
                            Err(e) => {
                              tracing::error!(err=%e);
                              continue;
                            },
                          }
                        };

                        for name in local_state.from.difference(&ping.from) {
                          tracing::info!("Hello {}!", name);
                        }
                      }
                    },
                    ContractResponse::PutResponse { key } => {
                      tracing::info!(key=%key, "Put ping contract successfully!");
                    },
                    ContractResponse::UpdateNotification { .. } => {},
                    ContractResponse::UpdateResponse { summary, key } => {
                      tracing::info!(key=%key, data=?summary, "received update response!");
                      let ping = match serde_json::from_slice::<Ping>(&summary) {
                        Ok(p) => p,
                        Err(e) => {
                          tracing::error!(err=%e, "failed to deserialize summary");
                          continue;
                        },
                      };

                      for name in local_state.from.difference(&ping.from) {
                        tracing::info!("Hello {}!", name);
                      }
                      local_state.from.extend(ping.from);
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
