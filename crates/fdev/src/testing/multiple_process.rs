use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    process::Stdio,
    time::Duration,
};

use anyhow::anyhow;
use freenet::dev_tool::{
    EventChain, InterProcessConnManager, MemoryEventsGen, NodeConfig, NodeLabel, PeerId, SimPeer,
};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use rand::Rng;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, Stdin},
    process::{ChildStdout, Command},
};

use super::Error;

impl super::TestConfig {
    fn subprocess_command(&self, seed: u64) -> Vec<String> {
        let mut args = Vec::new();

        args.push("test".to_owned());

        if let Some(name) = &self.name {
            args.push("--name".to_owned());
            args.push(name.to_string());
        }

        args.push("--seed".to_owned());
        args.push(seed.to_string());

        args.push("--gateways".to_owned());
        args.push(self.gateways.to_string());
        args.push("--nodes".to_owned());
        args.push(self.nodes.to_string());
        args.push("--ring-max-htl".to_owned());
        args.push(self.ring_max_htl.to_string());
        args.push("--rnd-if-htl-above".to_owned());
        args.push(self.rnd_if_htl_above.to_string());
        args.push("--max-connections".to_owned());
        args.push(self.max_connections.to_string());
        args.push("--min-connections".to_owned());
        args.push(self.min_connections.to_string());

        if let Some(start_backoff) = self.peer_start_backoff_ms {
            args.push("--peer-start-backoff-ms".to_owned());
            args.push(start_backoff.to_string());
        }

        if let Some(max_contract_number) = self.max_contract_number {
            args.push("--max_contract_number".to_owned());
            args.push(max_contract_number.to_string());
        }

        args.push("multi-process".to_owned());
        args.push("--mode".to_owned());
        args.push("child".to_owned());

        args
    }
}

#[derive(clap::Parser, Clone)]
pub struct MultiProcessConfig {
    #[arg(long, default_value_t = Process::Supervisor)]
    pub mode: Process,
    #[arg(long)]
    id: Option<usize>,
    #[arg(long)]
    data_dir: Option<String>,
}

#[derive(Default, Clone, clap::ValueEnum)]
pub enum Process {
    #[default]
    Supervisor,
    Child,
}

impl Display for Process {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Supervisor => write!(f, "supervisor"),
            Self::Child => write!(f, "child"),
        }
    }
}

pub(super) async fn run(
    config: &super::TestConfig,
    cmd_config: &MultiProcessConfig,
) -> anyhow::Result<(), Error> {
    match cmd_config.mode {
        Process::Supervisor => supervisor(config).await,
        Process::Child => child(config, cmd_config).await,
    }
}

async fn supervisor(config: &super::TestConfig) -> anyhow::Result<(), Error> {
    let mut simulated_network = super::config_sim_network(config).await?;
    simulated_network.debug(); // set to avoid deleting temp dirs created
    let peers = simulated_network.build_peers();

    let (user_ev_controller, event_rx) = tokio::sync::mpsc::channel(1);

    let seed = config.seed();
    let mut supervisor = Supervisor {
        processes: HashMap::new(),
        queued: HashMap::new(),
        sending: FuturesUnordered::new(),
        responses: FuturesUnordered::new(),
        event_rx: Some(event_rx),
    };
    let cmd_args = config.subprocess_command(seed);
    for (label, node) in &peers {
        let mut subprocess = SubProcess::start(&cmd_args, label, node.peer_id)?;
        subprocess.config(node).await?;
        supervisor.processes.insert(node.peer_id, subprocess);
    }

    let peers = peers
        .into_iter()
        .map(|(label, config)| (label, config.peer_id))
        .collect();
    let mut events = EventChain::new(peers, user_ev_controller, config.events, true);
    let next_event_wait_time = config
        .event_wait_ms
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(200));
    let (connectivity_timeout, network_connection_percent) = config.get_connection_check_params();
    let events_generated = tokio::task::spawn(async move {
        tracing::info!(
            "Waiting for network to be sufficiently connected ({}ms timeout, {}%)",
            connectivity_timeout.as_millis(),
            network_connection_percent * 100.0
        );
        simulated_network
            .check_partial_connectivity(connectivity_timeout, network_connection_percent)?;
        tracing::info!("Network is sufficiently connected, start sending events");
        while events.next().await.is_some() {
            tokio::time::sleep(next_event_wait_time).await;
        }
        Ok::<_, super::Error>(())
    });

    let ctrl_c = tokio::signal::ctrl_c();

    let supervisor_task = tokio::task::spawn(supervisor.start_simulation());

    tokio::pin!(events_generated);
    tokio::pin!(supervisor_task);
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut ctrl_c  /* SIGINT handling */ => {
                break;
            }
            res = &mut events_generated => {
                match res? {
                    Ok(()) => {
                        tracing::info!("Test events generated successfully");
                        *events_generated = tokio::task::spawn(futures::future::pending::<anyhow::Result<()>>());
                        continue;
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
            finalized = &mut supervisor_task => {
                match finalized? {
                    Ok(_) => {
                        tracing::info!("Test finalized successfully");
                        break;
                    }
                    Err(e) => {
                        tracing::error!("Test finalized with error: {}", e);
                        return Err(e);
                    }
                }
            }
        }
    }

    Ok(())
}

type ChildResponses = Vec<(PeerId, Vec<u8>)>;

/// Event driver for the supervisor process.
struct Supervisor {
    processes: HashMap<PeerId, SubProcess>,
    queued: HashMap<PeerId, VecDeque<IPCMessage>>,
    sending: FuturesUnordered<BoxFuture<'static, anyhow::Result<SubProcess>>>,
    responses: FuturesUnordered<BoxFuture<'static, anyhow::Result<(ChildStdout, ChildResponses)>>>,
    event_rx: Option<tokio::sync::mpsc::Receiver<(u32, PeerId)>>,
}

impl Supervisor {
    async fn start_simulation(mut self) -> anyhow::Result<()> {
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);

        let mut event_rx = self.event_rx.take().expect("should be set");
        let mut finished_events = false;

        for child_stdout in self
            .processes
            .values_mut()
            .map(|sp| sp.child.stdout.take().expect("should be set"))
        {
            self.responses
                .push(SubProcess::get_child_responses(child_stdout).boxed());
        }

        loop {
            tokio::select! {
                _ = &mut ctrl_c  /* SIGINT handling */ => {
                    break;
                }
                res = self.responses.next(), if !self.responses.is_empty() => {
                    let (child_stdout, responses) = match res {
                        Some(Ok(res)) => res,
                        Some(Err(err)) => {
                            tracing::error!("Error processing responses: {err}");
                            return Err(err);
                        }
                        None => {
                            continue;
                        }
                    };
                    self.responses.push(SubProcess::get_child_responses(child_stdout).boxed());
                    self.process_responses(responses);
                }
                completed_send = self.sending.next(), if !self.sending.is_empty() => {
                    let mut subprocess = match completed_send {
                        Some(Ok(res)) => res,
                        Some(Err(err)) => {
                            tracing::error!("Error sending message: {err}");
                            return Err(err);
                        }
                        None => {
                            continue;
                        }
                    };
                    let peer_queue = &mut *self.queued.entry(subprocess.id).or_default();
                    if !peer_queue.is_empty() {
                        let n = rand::thread_rng().gen_range(0..=peer_queue.len());
                        let messages = peer_queue.drain(..n).collect::<Vec<_>>();
                        let task = async move {
                            let stdin = subprocess.child.stdin.as_mut().expect("not taken");
                            tracing::debug!(peer = %subprocess.id, "Draining {} messages from queue", n + 1);
                            for pending in messages {
                                    pending.send(stdin).await?;
                            }
                            Ok(subprocess)
                        }.boxed();
                        self.sending.push(task);
                    } else {
                        self.processes.insert(subprocess.id, subprocess);
                    }
                }
                event = event_rx.recv(), if !finished_events => {
                    let Some((event, peer)) = event else {
                        tracing::info!("No more events");
                        finished_events = true;
                        continue;
                    };
                    let Some(mut subprocess) = self.processes.remove(&peer) else {
                        self.queued.entry(peer).or_default().push_back(IPCMessage::FiredEvent(event));
                        continue;
                    };
                    let mut pending = self.queued.remove(&peer).unwrap_or_default();
                    pending.push_back(IPCMessage::FiredEvent(event));
                    let task = async move {
                        for msg in pending {
                            msg.send(subprocess.child.stdin.as_mut().expect("not taken"))
                            .await?;
                        }
                        Ok(subprocess)
                    }.boxed();
                    self.sending.push(task);
                }
            }
        }
        for (_, subprocess) in self.processes.drain() {
            subprocess.close().await;
        }
        tracing::info!("Simulation finished");
        Ok(())
    }

    fn process_responses(&mut self, responses: ChildResponses) {
        for (target, data) in responses {
            if let Some(mut target) = self.processes.remove(&target) {
                tracing::debug!(to = %target.id, "Sending message");
                let task = async move {
                    target.send_msg(IPCMessage::Data(data)).await?;
                    Ok(target)
                }
                .boxed();
                self.sending.push(task);
            } else {
                tracing::debug!(%target, "Queuing message");
                self.queued
                    .entry(target)
                    .or_default()
                    .push_back(IPCMessage::Data(data));
            }
        }
    }
}

struct SubProcess {
    child: tokio::process::Child,
    id: PeerId,
}

impl SubProcess {
    fn start(cmd_args: &[String], label: &NodeLabel, id: PeerId) -> anyhow::Result<Self, Error> {
        // the identifier used for multi-process tests is the peer id
        let child = Command::new("fdev")
            .kill_on_drop(true)
            .args(cmd_args)
            .arg("--id")
            .arg(label.number().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(Self { child, id })
    }

    async fn config(&mut self, config: &NodeConfig) -> anyhow::Result<(), Error> {
        let input = self.get_input().ok_or_else(|| anyhow!("closed"))?;
        let serialize = bincode::serialize(&config)?;
        input
            .write_all(&(serialize.len() as u32).to_le_bytes())
            .await?;
        input.write_all(&serialize).await?;
        Ok(())
    }

    #[inline]
    async fn send_msg(&mut self, msg: IPCMessage) -> anyhow::Result<()> {
        msg.send(self.child.stdin.as_mut().expect("not taken"))
            .await?;
        Ok(())
    }

    async fn get_child_responses(
        mut stdout: ChildStdout,
    ) -> anyhow::Result<(ChildStdout, Vec<(PeerId, Vec<u8>)>)> {
        let mut returned = vec![];
        loop {
            match InterProcessConnManager::pull_msg(&mut stdout).await {
                Ok(Some((target, data))) => {
                    returned.push((target, data));
                }
                Ok(None) if !returned.is_empty() => {
                    break;
                }
                Ok(None) => {}
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
        tracing::debug!(len = %returned.len(), "Returning messages");
        Ok((stdout, returned))
    }

    async fn close(mut self) {
        let _ = self.child.kill().await;
    }

    #[inline]
    fn get_input(&mut self) -> Option<&mut tokio::process::ChildStdin> {
        self.child.stdin.as_mut()
    }
}

async fn child(
    test_config: &super::TestConfig,
    child_config: &MultiProcessConfig,
) -> anyhow::Result<()> {
    let id = child_config.id.expect("id should be set for child process");

    // write logs to stderr so stdout and stdin are free of unexpected data
    std::env::set_var("FREENET_LOG_TO_STDERR", "1");

    let (user_ev_controller, mut receiver_ch) = tokio::sync::watch::channel((0, PeerId::random()));
    receiver_ch.borrow_and_update();
    let mut input = BufReader::new(tokio::io::stdin());
    let node_config = Child::get_config(&mut input).await?;
    let this_child = Child {
        input,
        user_ev_controller,
        peer_id: node_config.peer_id,
    };
    std::env::set_var("FREENET_PEER_ID", node_config.peer_id.to_string());
    freenet::config::set_logger();
    let mut event_generator = MemoryEventsGen::<fastrand::Rng>::new_with_seed(
        receiver_ch.clone(),
        node_config.peer_id,
        test_config
            .seed
            .expect("seed should be set for child process"),
    );
    event_generator.rng_params(
        id,
        test_config.gateways + test_config.nodes,
        test_config
            .max_contract_number
            .unwrap_or(test_config.nodes * 10),
        test_config.events as usize,
    );
    let config = SimPeer::from(node_config);
    if let Some(backoff) = test_config.peer_start_backoff_ms {
        tokio::time::sleep(Duration::from_millis(backoff)).await;
    }
    tokio::task::spawn(this_child.event_loop());
    config.start_child(event_generator).await?;
    Ok(())
}

/// Controller for a child process.
struct Child {
    input: BufReader<Stdin>,
    user_ev_controller: tokio::sync::watch::Sender<(u32, PeerId)>,
    peer_id: PeerId,
}

impl Child {
    async fn get_config(input: &mut BufReader<Stdin>) -> anyhow::Result<NodeConfig> {
        let mut buf = [0u8; 4];
        input.read_exact(&mut buf).await?;
        let config_len = u32::from_le_bytes(buf);
        let mut config_buf = vec![0u8; config_len as usize];
        input.read_exact(&mut config_buf).await?;
        let node_config: NodeConfig = bincode::deserialize(&config_buf)?;
        Ok(node_config)
    }

    async fn event_loop(mut self) -> anyhow::Result<()> {
        loop {
            match IPCMessage::recv(&mut self.input).await {
                Err(err) => {
                    tracing::error!(
                        "Error at peer {} while receiving message: {err}",
                        self.peer_id
                    );
                    break Err(err);
                }
                Ok(IPCMessage::FiredEvent(id)) => {
                    self.user_ev_controller.send((id, self.peer_id))?;
                }
                Ok(IPCMessage::Data(data)) => {
                    InterProcessConnManager::push_msg(data);
                }
            }
        }
    }
}

enum IPCMessage {
    FiredEvent(u32),
    Data(Vec<u8>),
}

impl IPCMessage {
    async fn send(self, out: &mut tokio::process::ChildStdin) -> anyhow::Result<()> {
        match self {
            Self::FiredEvent(id) => {
                out.write_u8(0).await?;
                out.write_all(&id.to_le_bytes()).await?;
            }
            Self::Data(data) => {
                out.write_u8(1).await?;
                out.write_all(&(data.len() as u32).to_le_bytes()).await?;
                out.write_all(&data).await?;
            }
        }
        Ok(())
    }

    async fn recv(input: &mut BufReader<Stdin>) -> anyhow::Result<Self> {
        let marker = input.read_u8().await?;
        match marker {
            0 => {
                let mut buf = [0u8; 4];
                input.read_exact(&mut buf).await?;
                let event_id = u32::from_le_bytes(buf);
                Ok(Self::FiredEvent(event_id))
            }
            1 => {
                let mut buf = [0u8; 4];
                input.read_exact(&mut buf).await?;
                let data_len = u32::from_le_bytes(buf);
                let mut data = vec![0u8; data_len as usize];
                input.read_exact(&mut data).await?;
                Ok(Self::Data(data))
            }
            _ => unimplemented!(),
        }
    }
}
