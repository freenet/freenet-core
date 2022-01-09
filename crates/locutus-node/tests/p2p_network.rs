use std::{net::Ipv4Addr, time::Duration};

use libp2p::{identity::Keypair, PeerId};
use locutus_node::{
    Contract, ContractValue, InitPeerNode, Location, NodeConfig, UserEvent, UserEventsProxy,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
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

#[cfg(test)]
async fn start_new_peer(
    gateway_config: InitPeerNode,
    user_events: UserEvents,
) -> Result<(), anyhow::Error> {
    let mut config = NodeConfig::default();
    config.add_gateway(gateway_config);
    config.build()?.run(user_events).await
}

async fn run_test(manager: EventManager) -> Result<(), anyhow::Error> {
    tokio::time::sleep(Duration::from_secs(300)).await;

    let contract = Contract::new(vec![1, 2, 3, 4]);
    let key = contract.key();
    let init_val = ContractValue::new(vec![1, 2, 3, 4]);

    manager
        .tx_node_ev
        .send(UserEvent::Put {
            value: init_val,
            contract,
        })
        .await
        .map_err(|_| "channel closed")
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    manager
        .tx_gw_ev
        .send(UserEvent::Subscribe { key })
        .await
        .map_err(|_| "channel closed")
        .unwrap();

    tokio::time::sleep(Duration::from_secs(300)).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn node_communication() -> Result<(), anyhow::Error> {
    let gw_port = 64510;
    let gw_key = Keypair::generate_ed25519();
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
    tokio::spawn(start_gateway(
        gw_key,
        gw_port,
        gw_loc,
        UserEvents { rx_ev: rx_gw_ev },
    ));
    tokio::spawn(start_new_peer(gw_config, UserEvents { rx_ev: rx_node_ev }));
    run_test(manager).await
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
