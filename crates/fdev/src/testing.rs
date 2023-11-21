use std::time::Duration;

use anyhow::Error;
use freenet::dev_tool::SimNetwork;

pub(crate) use multiple_process::Process;

/// Testing framework for running Freenet network simulations.
#[derive(clap::Parser, Clone)]
pub struct TestConfig {
    /// Test name. If not provided, a random name will be generated.
    #[arg(long)]
    name: Option<String>,
    /// Seed to use when generating random data. If not provided, a random seed will be used.
    #[arg(long)]
    seed: Option<u64>,
    /// Number of total gateways for this test.
    #[arg(long, default_value_t = 2)]
    gateways: usize,
    /// Number of total regular peer nodes for this test.
    #[arg(long, default_value_t = 10)]
    nodes: usize,
    /// Max hops to live for operations (if it applies).
    #[arg(long, default_value_t = freenet::config::DEFAULT_MAX_HOPS_TO_LIVE)]
    ring_max_htl: usize,
    /// Default threshold for randomizing potential peers for new connections.
    #[arg(long, default_value_t = freenet::config::DEFAULT_RANDOM_PEER_CONN_THRESHOLD)]
    rnd_if_htl_above: usize,
    /// Maximum number of connections per peer.
    #[arg(long, default_value_t = freenet::config::DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,
    /// Minimum number of connections per peer.
    #[arg(long, default_value_t = freenet::config::DEFAULT_MIN_CONNECTIONS)]
    min_connections: usize,
    /// Maximum number of contracts live in the network.
    #[arg(long)]
    max_contract_number: Option<usize>,
    /// Number of events that will be executed in this simulation.
    /// Events are simulated get, puts and other operations.
    #[arg(long, default_value_t = u32::MAX)]
    events: u32,
    /// Time in milliseconds to wait for the network to be sufficiently connected to start sending events.
    /// (20% of the expected connections to be processed per gateway)
    #[arg(long)]
    wait_duration: Option<u64>,
    /// Time in milliseconds to wait for the next event to be executed.
    #[arg(long)]
    event_wait_time: Option<u64>,
    #[clap(subcommand)]
    /// Execution mode for the test.
    pub command: TestMode,
}

impl TestConfig {
    fn get_connection_check_params(&self) -> (Duration, f64) {
        let conns_per_gw = (self.nodes / self.gateways) as f64;
        let conn_percent = (conns_per_gw / self.nodes as f64).min(0.99);
        let connectivity_timeout = Duration::from_millis(self.wait_duration.unwrap_or_else(|| {
            // expect a peer to take max 200ms to connect, this should happen in parallel
            // but err on the side of safety
            (conns_per_gw * 200.0).ceil() as u64
        }));
        (connectivity_timeout, conn_percent)
    }

    fn seed(&self) -> u64 {
        use rand::RngCore;
        self.seed.unwrap_or_else(|| rand::rngs::OsRng.next_u64())
    }
}

fn randomize_test_name() -> String {
    const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz";
    use rand::seq::IteratorRandom;
    let mut rng = rand::thread_rng();
    let mut name = String::with_capacity(16);
    for _ in 0..16 {
        name.push(ALPHABET.chars().choose(&mut rng).expect("non empty"));
    }
    name
}

/// Under which mode will the test run execute.
#[derive(clap::Parser, Clone)]
pub enum TestMode {
    /// Runs multiple simulated nodes in a single process.
    SingleProcess,
    /// Runs multiple simulated nodes in multiple processes.
    MultiProcess(multiple_process::MultiProcessConfig),
    /// Runs multiple simulated nodes in multiple processes and multiple machines.
    Network,
}

pub(crate) async fn test_framework(base_config: TestConfig) -> anyhow::Result<(), Error> {
    match &base_config.command {
        TestMode::SingleProcess => {
            single_process::run(&base_config).await?;
        }
        TestMode::MultiProcess(config) => {
            multiple_process::run(&base_config, config).await?;
        }
        TestMode::Network => {
            todo!()
        }
    }
    Ok(())
}

async fn config_sim_network(base_config: &TestConfig) -> anyhow::Result<SimNetwork, Error> {
    if base_config.gateways == 0 {
        anyhow::bail!("Gateways should be higher than 0");
    }
    if base_config.nodes == 0 {
        anyhow::bail!("Nodes should be higher than 0");
    }
    let name = &base_config
        .name
        .as_ref()
        .cloned()
        .unwrap_or_else(randomize_test_name);
    let sim = SimNetwork::new(
        name,
        base_config.gateways,
        base_config.nodes,
        base_config.ring_max_htl,
        base_config.rnd_if_htl_above,
        base_config.max_connections,
        base_config.min_connections,
    )
    .await;
    Ok(sim)
}

mod multiple_process {
    use std::{
        collections::{HashMap, VecDeque},
        fmt::Display,
        process::Stdio,
        time::Duration,
    };

    use anyhow::anyhow;
    use freenet::dev_tool::{
        EventChain, InterProcessConnManager, MemoryEventsGen, NodeConfig, NodeLabel, PeerId,
        SimPeer,
    };
    use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, BufReader, Stdin},
        process::Command,
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
            Process::Child => {
                child(config, cmd_config.id.expect("id should be set for child")).await
            }
        }
    }

    async fn supervisor(config: &super::TestConfig) -> anyhow::Result<(), Error> {
        let mut simulated_network = super::config_sim_network(config).await?;
        simulated_network.debug(); // set to avoid deleting temp dirs created
        let peers = simulated_network.build_peers();

        // let (user_ev_controller, event_rx) = tokio::sync::watch::channel((0, PeerId::random()));
        let (user_ev_controller, event_rx) = tokio::sync::mpsc::channel(1);

        let seed = config.seed();
        let mut supervisor = Supervisor {
            processes: HashMap::new(),
            event_rx: Some(event_rx),
            waiting_response: FuturesUnordered::new(),
            queued: HashMap::new(),
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
            .event_wait_time
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(200));
        let (connectivity_timeout, network_connection_percent) =
            config.get_connection_check_params();
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
        event_rx: Option<tokio::sync::mpsc::Receiver<(u32, PeerId)>>,
        waiting_response:
            FuturesUnordered<BoxFuture<'static, anyhow::Result<(SubProcess, ChildResponses)>>>,
        queued: HashMap<PeerId, VecDeque<IPCMessage>>,
    }

    impl Supervisor {
        async fn start_simulation(mut self) -> anyhow::Result<()> {
            let ctrl_c = tokio::signal::ctrl_c();
            tokio::pin!(ctrl_c);

            let mut event_rx = self.event_rx.take().expect("should be set");
            let mut finished_events = false;

            loop {
                tokio::select! {
                    _ = &mut ctrl_c  /* SIGINT handling */ => {
                        break;
                    }
                    res = self.waiting_response.next() => {
                        let (subprocess, responses) = match res {
                            Some(Ok(res)) => res,
                            Some(Err(err)) => {
                                tracing::error!("Error processing responses: {err}");
                                return Err(err);
                            }
                            None => {
                                continue;
                            }
                        };
                        if let Err(err) = self.process_responses(subprocess, responses).await {
                            tracing::error!("Error processing responses: {err}");
                            return Err(err);
                        }

                    }
                    event = event_rx.recv(), if !finished_events => {
                        let Some((event, peer)) = event else {
                            tracing::info!("No more events");
                            finished_events = true;
                            continue;
                        };
                        tracing::debug!("Event {event} fired for {peer}");
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
                            let mut returned = vec![];
                            while let Ok((target, msg)) = InterProcessConnManager::pull_msg(
                                subprocess.child.stdout.as_mut().expect("stdout not taken"),
                            )
                            .await
                            {
                                returned.push((target, msg));
                            }
                            Ok((subprocess, returned))
                        }.boxed();
                        self.waiting_response.push(task);
                    }
                }
            }
            for (_, subprocess) in self.processes.drain() {
                subprocess.close().await;
            }
            tracing::info!("Simulation finished");
            Ok(())
        }

        async fn process_responses(
            &mut self,
            mut subprocess: SubProcess,
            responses: ChildResponses,
        ) -> anyhow::Result<()> {
            for (target, data) in responses {
                if let Some(target) = self.processes.remove(&target) {
                    tracing::debug!(to = %subprocess.id, "sending message");
                    let task = async move { target.send_msg(IPCMessage::Data(data)).await }.boxed();
                    self.waiting_response.push(task);
                } else {
                    self.queued
                        .entry(target)
                        .or_default()
                        .push_back(IPCMessage::Data(data));
                }
            }
            if let Some(mut queued) = self.queued.remove(&subprocess.id) {
                for msg in queued.drain(..) {
                    msg.send(subprocess.child.stdin.as_mut().expect("not taken"))
                        .await?;
                    while let Ok((target, data)) = InterProcessConnManager::pull_msg(
                        subprocess.child.stdout.as_mut().expect("stdout not taken"),
                    )
                    .await
                    {
                        self.queued
                            .entry(target)
                            .or_default()
                            .push_back(IPCMessage::Data(data));
                    }
                }
            }
            self.processes.insert(subprocess.id, subprocess);
            Ok(())
        }
    }

    struct SubProcess {
        child: tokio::process::Child,
        id: PeerId,
    }

    impl SubProcess {
        fn start(
            cmd_args: &[String],
            label: &NodeLabel,
            id: PeerId,
        ) -> anyhow::Result<Self, Error> {
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

        async fn send_msg(mut self, msg: IPCMessage) -> anyhow::Result<(Self, ChildResponses)> {
            msg.send(self.child.stdin.as_mut().expect("not taken"))
                .await?;
            let mut returned = vec![];
            while let Ok((target, msg)) = InterProcessConnManager::pull_msg(
                self.child.stdout.as_mut().expect("stdout not taken"),
            )
            .await
            {
                returned.push((target, msg));
            }
            Ok((self, returned))
        }

        async fn close(mut self) {
            let _ = self.child.kill().await;
        }

        #[inline]
        fn get_input(&mut self) -> Option<&mut tokio::process::ChildStdin> {
            self.child.stdin.as_mut()
        }
    }

    async fn child(config: &super::TestConfig, id: usize) -> anyhow::Result<()> {
        // write logs to stderr so stdout and stdin are free of unexpected data
        std::env::set_var("FREENET_LOG_TO_STDERR", "1");
        freenet::config::set_logger();

        let (user_ev_controller, mut receiver_ch) =
            tokio::sync::watch::channel((0, PeerId::random()));
        receiver_ch.borrow_and_update();
        let mut input = BufReader::new(tokio::io::stdin());
        let node_config = Child::get_config(&mut input).await?;
        let this_child = Child {
            input,
            user_ev_controller,
            peer_id: node_config.peer_id,
        };
        let mut event_generator = MemoryEventsGen::<fastrand::Rng>::new_with_seed(
            receiver_ch.clone(),
            node_config.peer_id,
            config.seed.expect("seed should be set for child process"),
        );
        event_generator.rng_params(
            id,
            config.gateways + config.nodes,
            config.max_contract_number.unwrap_or(config.nodes * 10),
            config.events as usize,
        );
        let config = SimPeer::from(node_config);
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
                        tracing::debug!("Event {id} received");
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
                    tracing::info!("Sending data");
                    out.write_u8(1).await?;
                    out.write_all(&(data.len() as u32).to_le_bytes()).await?;
                    out.write_all(&data).await?;
                    tracing::info!("Sent data");
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
}

mod single_process {
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::signal;

    pub(super) async fn run(config: &super::TestConfig) -> anyhow::Result<(), super::Error> {
        let mut simulated_network = super::config_sim_network(config).await?;

        let join_handles = simulated_network
            .start_with_rand_gen::<fastrand::Rng>(
                config.seed(),
                config.max_contract_number.unwrap_or(config.nodes * 10),
                config.events as usize,
            )
            .await;

        let events = config.events;
        let next_event_wait_time = config
            .event_wait_time
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(200));
        let (connectivity_timeout, network_connection_percent) =
            config.get_connection_check_params();
        let events_generated = tokio::task::spawn(async move {
            tracing::info!(
                "Waiting for network to be sufficiently connected ({}ms timeout, {}%)",
                connectivity_timeout.as_millis(),
                network_connection_percent * 100.0
            );
            simulated_network
                .check_partial_connectivity(connectivity_timeout, network_connection_percent)?;
            let mut stream = simulated_network.event_chain(events, None);
            while stream.next().await.is_some() {
                tokio::time::sleep(next_event_wait_time).await;
            }
            Ok::<_, super::Error>(())
        });

        let join_peer_tasks = async move {
            let mut futs = futures::stream::FuturesUnordered::from_iter(join_handles);
            while let Some(join_handle) = futs.next().await {
                join_handle??;
            }
            Ok::<_, super::Error>(())
        };

        let ctrl_c = signal::ctrl_c();

        tokio::pin!(events_generated);
        tokio::pin!(join_peer_tasks);
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
                finalized = &mut join_peer_tasks => {
                    match finalized {
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
}
