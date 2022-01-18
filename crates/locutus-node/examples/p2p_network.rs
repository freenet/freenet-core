use std::{net::Ipv4Addr, time::Duration};

use anyhow::{anyhow, bail};
use libp2p::{
    identity::{ed25519, Keypair},
    PeerId,
};
use locutus_node::{
    Contract, ContractValue, InitPeerNode, Location, NodeConfig, UserEvent, UserEventsProxy,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

const ENCODED_GW_KEY: &[u8] = include_bytes!("gw_key");

async fn start_gateway(
    key: Keypair,
    port: u16,
    location: Location,
    user_events: UserEvents,
) -> Result<(), anyhow::Error> {
    let mut config = NodeConfig::default();
    config
        .with_ip(Ipv4Addr::LOCALHOST)
        .with_port(port)
        .with_key(key)
        .with_location(location);
    config.build()?.run(user_events).await
}

async fn start_new_peer(
    gateway_config: InitPeerNode,
    user_events: UserEvents,
) -> Result<(), anyhow::Error> {
    let mut config = NodeConfig::default();
    config.add_gateway(gateway_config);
    config.build()?.run(user_events).await
}

async fn run_test(manager: EventManager) -> Result<(), anyhow::Error> {
    let contract = Contract::new(vec![7, 3, 9, 5]);
    let key = contract.key();
    let init_val = ContractValue::new(vec![1, 2, 3, 4]);

    tokio::time::sleep(Duration::from_secs(10)).await;
    manager
        .tx_gw_ev
        .send(UserEvent::Put {
            value: init_val,
            contract: contract.clone(),
        })
        .await
        .map_err(|_| anyhow!("channel closed"))?;
    tokio::time::sleep(Duration::from_secs(10)).await;

    manager
        .tx_gw_ev
        .send(UserEvent::Get {
            key,
            contract: false,
        })
        .await
        .map_err(|_| anyhow!("channel closed"))?;
    tokio::time::sleep(Duration::from_secs(10)).await;

    let second_val = ContractValue::new(vec![2, 3, 1, 4]);
    manager
        .tx_node_ev
        .send(UserEvent::Put {
            value: second_val,
            contract,
        })
        .await
        .map_err(|_| anyhow!("channel closed"))?;
    tokio::time::sleep(Duration::from_secs(300)).await;
    Ok(())
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), anyhow::Error> {
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
    tx_gw_ev: Sender<UserEvent>,
    tx_node_ev: Sender<UserEvent>,
}

struct UserEvents {
    rx_ev: Receiver<UserEvent>,
}

#[async_trait::async_trait]
impl UserEventsProxy for UserEvents {
    async fn recv(&mut self) -> locutus_node::UserEvent {
        self.rx_ev.recv().await.expect("channel open")
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
