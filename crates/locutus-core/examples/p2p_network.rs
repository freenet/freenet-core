use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::{anyhow, bail};
use futures::future::BoxFuture;
use libp2p::{
    identity::{ed25519, Keypair},
    PeerId,
};
use locutus_core::*;
use locutus_runtime::prelude::{ContractContainer, WasmAPIVersion, WrappedContract, WrappedState};
use locutus_stdlib::{
    api::{ClientError, ClientRequest, ContractRequest, HostResponse},
    prelude::{ContractCode, Parameters},
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

const ENCODED_GW_KEY: &[u8] = include_bytes!("gw_key");

async fn start_gateway(
    key: Keypair,
    port: u16,
    location: Location,
    user_events: UserEvents,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // todo: send user events though ws interface
    let mut config = NodeConfig::new([Box::new(user_events)]);
    config
        .with_ip(Ipv4Addr::LOCALHOST)
        .with_port(port)
        .with_key(key)
        .with_location(location);
    config.build()?.run().await
}

async fn start_new_peer(
    gateway_config: InitPeerNode,
    user_events: UserEvents,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // todo: send user events though ws interface
    let mut config = NodeConfig::new([Box::new(user_events)]);
    config.add_gateway(gateway_config);
    config.build()?.run().await
}

async fn run_test(manager: EventManager) -> Result<(), anyhow::Error> {
    let contract = ContractContainer::Wasm(WasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(vec![7, 3, 9, 5])),
        Parameters::from(vec![]),
    )));
    let key = contract.key().clone();
    let init_val = WrappedState::new(vec![1, 2, 3, 4]);

    tokio::time::sleep(Duration::from_secs(10)).await;
    manager
        .tx_gw_ev
        .send(
            ContractRequest::Put {
                state: init_val,
                contract: contract.clone(),
                related_contracts: Default::default(),
            }
            .into(),
        )
        .await
        .map_err(|_| anyhow!("channel closed"))?;
    tokio::time::sleep(Duration::from_secs(10)).await;

    manager
        .tx_gw_ev
        .send(
            ContractRequest::Get {
                key,
                fetch_contract: false,
            }
            .into(),
        )
        .await
        .map_err(|_| anyhow!("channel closed"))?;
    tokio::time::sleep(Duration::from_secs(10)).await;

    let second_val = WrappedState::new(vec![2, 3, 1, 4]);
    manager
        .tx_node_ev
        .send(
            ContractRequest::Put {
                state: second_val,
                contract,
                related_contracts: Default::default(),
            }
            .into(),
        )
        .await
        .map_err(|_| anyhow!("channel closed"))?;
    tokio::time::sleep(Duration::from_secs(300)).await;
    Ok(())
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    let args = Args::parse_args()?;

    let gw_port = 64510;
    let gw_key: Keypair = Keypair::Ed25519(ed25519::Keypair::decode(&mut ENCODED_GW_KEY.to_vec())?);
    let gw_id: PeerId = gw_key.public().into();
    let gw_loc = Location::random();
    let gw_config = InitPeerNode::new(gw_id, gw_loc)
        .listening_ip(Ipv4Addr::LOCALHOST)
        .listening_port(gw_port);

    let (tx_gw_ev, rx_gw_ev) = channel(100);
    let (tx_node_ev, rx_node_ev) = channel(100);
    let manager = EventManager {
        tx_gw_ev,
        tx_node_ev,
    };

    match (args.is_gw, args.is_peer) {
        (true, true) => bail!("a node cannot be both a gateway and a normal peer"),
        (true, _) => {
            tokio::spawn(start_gateway(
                gw_key,
                gw_port,
                gw_loc,
                UserEvents { rx_ev: rx_gw_ev },
            ));
            run_test(manager).await
        }
        (_, true) => {
            tokio::spawn(start_new_peer(gw_config, UserEvents { rx_ev: rx_node_ev }));
            run_test(manager).await
        }
        _ => {
            tokio::spawn(start_gateway(
                gw_key,
                gw_port,
                gw_loc,
                UserEvents { rx_ev: rx_gw_ev },
            ));
            tokio::time::sleep(Duration::from_millis(100)).await;
            tokio::spawn(start_new_peer(gw_config, UserEvents { rx_ev: rx_node_ev }));
            run_test(manager).await
        }
    }
}

#[derive(Clone)]
struct EventManager {
    tx_gw_ev: Sender<ClientRequest<'static>>,
    tx_node_ev: Sender<ClientRequest<'static>>,
}

struct UserEvents {
    rx_ev: Receiver<ClientRequest<'static>>,
}

impl ClientEventsProxy for UserEvents {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        Box::pin(async move {
            Ok(OpenRequest::new(
                ClientId::FIRST,
                self.rx_ev.recv().await.expect("channel open"),
            ))
        })
    }

    /// Sends a response from the host to the client application.
    fn send(
        &mut self,
        _id: ClientId,
        _response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        Box::pin(async move {
            log::info!("received response");
            Ok(())
        })
    }
}

struct Args {
    is_gw: bool,
    is_peer: bool,
}

impl Args {
    fn parse_args() -> Result<Self, anyhow::Error> {
        let mut pargs = pico_args::Arguments::from_env();
        Ok(Args {
            is_gw: pargs.contains("--gateway"),
            is_peer: pargs.contains("--peer"),
        })
    }
}
