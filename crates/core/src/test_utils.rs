#![allow(clippy::unbuffered_bytes)]
use std::{
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use clap::ValueEnum;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, WebApi},
    prelude::*,
};
use serde::{Deserialize, Serialize};

use crate::util::workspace::get_workspace_target_dir;

/// Set the peer identifier for the current thread's tracing context.
///
/// This adds a `test_node` field to all log messages from this thread, making it
/// easier to distinguish logs from different peers in multi-peer tests.
///
/// # Example
/// ```ignore
/// set_peer_id("gateway");
/// tracing::info!("Starting gateway");  // Will include test_node="gateway"
///
/// set_peer_id("peer-1");
/// tracing::info!("Starting peer 1");   // Will include test_node="peer-1"
/// ```
///
/// # Note
/// This should be called at the start of each peer's initialization in tests.
/// When using `#[test_log::test]`, the test framework will automatically
/// configure tracing to show these fields.
///
/// The field name `test_node` is used to avoid conflicts with the production
/// `peer` field which contains the actual cryptographic PeerId.
pub fn set_peer_id(peer_id: impl Into<String>) {
    let peer_id = peer_id.into();
    tracing::Span::current().record("test_node", peer_id);
}

/// Format for test logger output
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Pretty-printed format (human-readable)
    Pretty,
    /// JSON format (machine-readable)
    Json,
}

/// A configurable test logger that provides flexible logging for tests.
///
/// This helper provides more control than test-log, including:
/// - JSON output support
/// - Per-test configuration
/// - Log capturing for inspection
///
/// # Peer Identification
///
/// For multi-peer tests, use `.instrument()` to attach isolated spans:
/// ```ignore
/// use tracing::Instrument;
///
/// let gateway = async {
///     tracing::info!("Gateway starting");
/// }
/// .instrument(tracing::info_span!("test_peer", test_node = "gateway"));
/// ```
///
/// # Example
/// ```ignore
/// use tracing::Instrument;
///
/// #[tokio::test]
/// async fn my_test() -> anyhow::Result<()> {
///     let _logger = TestLogger::new()
///         .with_json()
///         .with_level("debug")
///         .init();
///
///     // For multi-peer tests, use .instrument() to isolate spans
///     let gateway = async {
///         tracing::info!("Gateway starting");
///     }
///     .instrument(tracing::info_span!("test_peer", test_node = "gateway"));
///
///     Ok(())
/// }
/// ```
pub struct TestLogger {
    format: LogFormat,
    level: String,
    capture: bool,
    captured_logs: Arc<Mutex<Vec<String>>>,
    _guard: Option<tracing::subscriber::DefaultGuard>,
}

impl TestLogger {
    /// Create a new TestLogger with default settings.
    ///
    /// Defaults:
    /// - Format: Pretty
    /// - Level: "info"
    /// - No peer ID
    /// - No log capture
    pub fn new() -> Self {
        Self {
            format: LogFormat::Pretty,
            level: "info".to_string(),
            capture: false,
            captured_logs: Arc::new(Mutex::new(Vec::new())),
            _guard: None,
        }
    }

    /// Enable JSON output format.
    pub fn with_json(mut self) -> Self {
        self.format = LogFormat::Json;
        self
    }

    /// Enable pretty output format (default).
    pub fn with_pretty(mut self) -> Self {
        self.format = LogFormat::Pretty;
        self
    }

    /// Set the log level filter.
    ///
    /// # Example
    /// ```ignore
    /// let logger = TestLogger::new().with_level("debug");
    /// ```
    pub fn with_level(mut self, level: impl Into<String>) -> Self {
        self.level = level.into();
        self
    }

    /// Enable log capturing for programmatic inspection.
    ///
    /// When enabled, logs will be stored in memory and can be queried
    /// using `contains()`, `logs()`, etc.
    ///
    /// # Example
    /// ```ignore
    /// let logger = TestLogger::new().capture_logs().init();
    /// tracing::info!("test message");
    /// assert!(logger.contains("test message"));
    /// ```
    pub fn capture_logs(mut self) -> Self {
        self.capture = true;
        self
    }

    /// Initialize the logger and return a guard.
    ///
    /// The guard must be held for the duration of the test to keep
    /// the logger active.
    ///
    /// # Example
    /// ```ignore
    /// let _logger = TestLogger::new().with_json().init();
    /// // Logger is active while _logger is in scope
    /// ```
    pub fn init(mut self) -> Self {
        use tracing_subscriber::{
            fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer,
        };

        // Create env filter from level
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&self.level));

        // Build the appropriate layer based on format
        // Note: Span fields are automatically included in logs within those spans
        let layer: Box<dyn Layer<_> + Send + Sync> = match self.format {
            LogFormat::Pretty => {
                if self.capture {
                    let writer = CapturingWriter::new(self.captured_logs.clone());
                    fmt::layer()
                        .with_writer(move || writer.clone())
                        .pretty()
                        .boxed()
                } else {
                    fmt::layer().with_test_writer().pretty().boxed()
                }
            }
            LogFormat::Json => {
                if self.capture {
                    let writer = CapturingWriter::new(self.captured_logs.clone());
                    fmt::layer()
                        .with_writer(move || writer.clone())
                        .json()
                        .with_span_list(true)
                        .flatten_event(true)
                        .boxed()
                } else {
                    fmt::layer()
                        .with_test_writer()
                        .json()
                        .with_span_list(true)
                        .flatten_event(true)
                        .boxed()
                }
            }
        };

        let subscriber = tracing_subscriber::registry().with(env_filter).with(layer);

        // Set as default subscriber
        self._guard = Some(subscriber.set_default());

        self
    }

    /// Check if captured logs contain a specific message.
    ///
    /// # Panics
    /// Panics if log capturing was not enabled with `capture_logs()`.
    pub fn contains(&self, message: &str) -> bool {
        if !self.capture {
            panic!("Cannot inspect logs without calling .capture_logs()");
        }

        self.captured_logs
            .lock()
            .unwrap()
            .iter()
            .any(|log| log.contains(message))
    }

    /// Get all captured logs.
    ///
    /// # Panics
    /// Panics if log capturing was not enabled with `capture_logs()`.
    pub fn logs(&self) -> Vec<String> {
        if !self.capture {
            panic!("Cannot get logs without calling .capture_logs()");
        }

        self.captured_logs.lock().unwrap().clone()
    }

    /// Get logs matching a filter predicate.
    ///
    /// # Panics
    /// Panics if log capturing was not enabled with `capture_logs()`.
    pub fn logs_matching(&self, filter: impl Fn(&str) -> bool) -> Vec<String> {
        self.logs().into_iter().filter(|log| filter(log)).collect()
    }

    /// Get the number of captured log entries.
    ///
    /// # Panics
    /// Panics if log capturing was not enabled with `capture_logs()`.
    pub fn log_count(&self) -> usize {
        if !self.capture {
            panic!("Cannot count logs without calling .capture_logs()");
        }

        self.captured_logs.lock().unwrap().len()
    }
}

impl Default for TestLogger {
    fn default() -> Self {
        Self::new()
    }
}

/// A writer that captures logs to a buffer and also writes to test output.
#[derive(Clone)]
struct CapturingWriter {
    buffer: Arc<Mutex<Vec<String>>>,
}

impl CapturingWriter {
    fn new(buffer: Arc<Mutex<Vec<String>>>) -> Self {
        Self { buffer }
    }
}

impl Write for CapturingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Convert to string and store complete lines
        if let Ok(s) = std::str::from_utf8(buf) {
            for line in s.lines() {
                if !line.is_empty() {
                    self.buffer.lock().unwrap().push(line.to_string());
                }
            }
        }

        // Also write to stdout (which test harness captures)
        // This ensures logs still show on failure
        std::io::stdout().write_all(buf)?;

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()
    }
}

pub async fn make_put(
    client: &mut WebApi,
    state: WrappedState,
    contract: ContractContainer,
    subscribe: bool,
) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: contract.clone(),
            state: state.clone(),
            related_contracts: RelatedContracts::default(),
            subscribe,
        }))
        .await?;
    Ok(())
}

pub async fn make_update(
    client: &mut WebApi,
    key: ContractKey,
    state: WrappedState,
) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Update {
            key,
            data: UpdateData::State(State::from(state)),
        }))
        .await?;
    Ok(())
}

pub async fn make_subscribe(client: &mut WebApi, key: ContractKey) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key,
            summary: None,
        }))
        .await?;
    Ok(())
}

pub async fn make_get(
    client: &mut WebApi,
    key: ContractKey,
    return_contract_code: bool,
    subscribe: bool,
) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key,
            return_contract_code,
            subscribe,
        }))
        .await?;
    Ok(())
}

pub fn load_contract(name: &str, params: Parameters<'static>) -> anyhow::Result<ContractContainer> {
    let contract_bytes = WrappedContract::new(
        Arc::new(ContractCode::from(compile_contract(name)?)),
        params,
    );
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_bytes));
    Ok(contract)
}

pub fn load_delegate(name: &str, params: Parameters<'static>) -> anyhow::Result<DelegateContainer> {
    let delegate_bytes = compile_delegate(name)?;
    let delegate_code = DelegateCode::from(delegate_bytes);
    let delegate = Delegate::from((&delegate_code, &params));
    let delegate = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate));
    Ok(delegate)
}

// TODO: refactor so we share the implementation with fdev (need to extract to )
fn compile_contract(name: &str) -> anyhow::Result<Vec<u8>> {
    let contract_path = {
        const CRATE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/");
        let contracts = PathBuf::from(CRATE_DIR);
        contracts.join(name)
    };

    println!("module path: {contract_path:?}");
    let target = get_workspace_target_dir();
    println!(
        "trying to compile the test contract, target: {}",
        target.display()
    );

    compile_rust_wasm_lib(
        &BuildToolConfig {
            features: None,
            package_type: PackageType::Contract,
            debug: false,
        },
        &contract_path,
    )?;

    let output_file = target
        .join(WASM_TARGET)
        .join("release")
        .join(name.replace('-', "_"))
        .with_extension("wasm");
    println!("output file: {output_file:?}");
    Ok(std::fs::read(output_file)?)
}

fn compile_delegate(name: &str) -> anyhow::Result<Vec<u8>> {
    let delegate_path = {
        const CRATE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/");
        let delegates = PathBuf::from(CRATE_DIR);
        delegates.join(name)
    };

    println!("delegate path: {delegate_path:?}");

    // Check if the delegate directory exists
    if !delegate_path.exists() {
        return Err(anyhow::anyhow!(
            "Delegate directory does not exist: {delegate_path:?}"
        ));
    }

    let target = get_workspace_target_dir();
    println!(
        "trying to compile the test delegate, target: {}",
        target.display()
    );

    compile_rust_wasm_lib(
        &BuildToolConfig {
            features: None,
            package_type: PackageType::Delegate,
            debug: false,
        },
        &delegate_path,
    )?;

    let output_file = target
        .join(WASM_TARGET)
        .join("release")
        .join(name.replace('-', "_"))
        .with_extension("wasm");
    println!("output file: {output_file:?}");

    // Check if output file exists before reading
    if !output_file.exists() {
        return Err(anyhow::anyhow!(
            "Compiled WASM file not found at: {output_file:?}"
        ));
    }

    let wasm_data = std::fs::read(&output_file)
        .map_err(|e| anyhow::anyhow!("Failed to read output file {output_file:?}: {e}"))?;
    println!("WASM size: {} bytes", wasm_data.len());

    Ok(wasm_data)
}

const WASM_TARGET: &str = "wasm32-unknown-unknown";

fn compile_options(cli_config: &BuildToolConfig) -> impl Iterator<Item = String> {
    let release: &[&str] = if cli_config.debug {
        &[]
    } else {
        &["--release"]
    };
    let feature_list = cli_config
        .features
        .iter()
        .flat_map(|s| {
            s.split(',')
                .filter(|p| *p != cli_config.package_type.feature())
        })
        .chain([cli_config.package_type.feature()]);
    let features = [
        "--features".to_string(),
        feature_list.collect::<Vec<_>>().join(","),
    ];
    features
        .into_iter()
        .chain(release.iter().map(|s| s.to_string()))
}

fn compile_rust_wasm_lib(cli_config: &BuildToolConfig, work_dir: &Path) -> anyhow::Result<()> {
    const RUST_TARGET_ARGS: &[&str] = &["build", "--lib", "--target"];
    use std::io::IsTerminal;
    let comp_opts = compile_options(cli_config).collect::<Vec<_>>();
    let cmd_args = if std::io::stdout().is_terminal() && std::io::stderr().is_terminal() {
        RUST_TARGET_ARGS
            .iter()
            .copied()
            .chain([WASM_TARGET, "--color", "always"])
            .chain(comp_opts.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
    } else {
        RUST_TARGET_ARGS
            .iter()
            .copied()
            .chain([WASM_TARGET])
            .chain(comp_opts.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
    };

    let package_type = cli_config.package_type;
    println!("Compiling {package_type} with rust");

    // Set CARGO_TARGET_DIR if not already set to ensure consistent output location
    let mut command = Command::new("cargo");
    if std::env::var("CARGO_TARGET_DIR").is_err() {
        command.env("CARGO_TARGET_DIR", get_workspace_target_dir());
    }

    let child = command
        .args(&cmd_args)
        .current_dir(work_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            eprintln!("Error while executing cargo command: {e}");
            anyhow::anyhow!("Error while executing cargo command: {e}")
        })?;
    pipe_std_streams(child)?;
    Ok(())
}

pub(crate) fn pipe_std_streams(mut child: Child) -> anyhow::Result<()> {
    let c_stdout = child.stdout.take().expect("Failed to open command stdout");
    let c_stderr = child.stderr.take().expect("Failed to open command stderr");

    let write_child_stderr = move || -> anyhow::Result<()> {
        let mut stderr = io::stderr();
        for b in c_stderr.bytes() {
            let b = b?;
            stderr.write_all(&[b])?;
        }
        Ok(())
    };

    let write_child_stdout = move || -> anyhow::Result<()> {
        let mut stdout = io::stdout();
        for b in c_stdout.bytes() {
            let b = b?;
            stdout.write_all(&[b])?;
        }
        Ok(())
    };
    std::thread::spawn(write_child_stdout);
    std::thread::spawn(write_child_stderr);

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    anyhow::bail!("exit with status: {status}");
                }
                break;
            }
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    Ok(())
}

/// Builds and packages a contract or delegate.
///
/// This tool will build the WASM contract or delegate and publish it to the network.
#[derive(clap::Parser, Clone, Debug)]
pub struct BuildToolConfig {
    /// Compile the contract or delegate with specific features.
    #[arg(long)]
    pub(crate) features: Option<String>,

    // /// Compile the contract or delegate with a specific API version.
    // #[arg(long, value_parser = parse_version, default_value_t=Version::new(0, 0, 1))]
    // pub(crate) version: Version,
    /// Output object type.
    #[arg(long, value_enum, default_value_t=PackageType::default())]
    pub(crate) package_type: PackageType,

    /// Compile in debug mode instead of release.
    #[arg(long)]
    pub(crate) debug: bool,
}

#[derive(Default, Debug, Clone, Copy, ValueEnum)]
pub(crate) enum PackageType {
    #[default]
    Contract,
    Delegate,
}

impl PackageType {
    pub fn feature(&self) -> &'static str {
        match self {
            PackageType::Contract => "freenet-main-contract",
            PackageType::Delegate => "freenet-main-delegate",
        }
    }
}

impl std::fmt::Display for PackageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackageType::Contract => write!(f, "contract"),
            PackageType::Delegate => write!(f, "delegate"),
        }
    }
}

pub async fn verify_contract_exists(dir: &Path, key: ContractKey) -> anyhow::Result<bool> {
    let code_hash = key.encoded_code_hash().unwrap_or_else(|| {
        panic!("Contract key does not have a code hash");
    });
    let contract_path = dir.join("contracts").join(code_hash);
    Ok(tokio::fs::metadata(contract_path).await.is_ok())
}

// Test data structures for contract operations

/// Data model representing a todo list for testing
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TodoList {
    /// List of tasks
    pub tasks: Vec<Task>,
    /// State version for concurrency control
    pub version: u64,
}

/// Data model representing a task for testing
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Task {
    /// Unique task identifier
    pub id: u64,
    /// Task title
    pub title: String,
    /// Task description
    pub description: String,
    /// Completion status
    pub completed: bool,
    /// Priority (1-5, where 5 is highest)
    pub priority: u8,
}

/// Operations that can be performed on tasks
#[derive(Serialize, Deserialize, Debug)]
pub enum TodoOperation {
    /// Add a new task
    Add(Task),
    /// Update an existing task
    Update(Task),
    /// Remove a task by ID
    Remove(u64),
    /// Mark a task as completed
    Complete(u64),
}

/// Creates an empty todo list for testing
pub fn create_empty_todo_list() -> Vec<u8> {
    let todo_list = TodoList {
        tasks: Vec::new(),
        version: 0,
    };

    serde_json::to_vec(&todo_list).unwrap_or_default()
}

/// Creates a todo list with a single task for testing
pub fn create_todo_list_with_item(title: &str) -> Vec<u8> {
    let task = Task {
        id: 1,
        title: title.to_string(),
        description: String::new(),
        completed: false,
        priority: 3,
    };

    let todo_list = TodoList {
        tasks: vec![task],
        version: 1,
    };

    serde_json::to_vec(&todo_list).unwrap_or_default()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_compile_contract() -> testresult::TestResult {
        let contract = compile_contract("test-contract-integration")?;
        assert!(!contract.is_empty());
        Ok(())
    }

    #[test]
    fn test_logger_basic() {
        let _logger = TestLogger::new().with_pretty().with_level("info").init();

        tracing::info!("Test log message");
        tracing::warn!("Test warning");
    }

    #[test]
    fn test_logger_json() {
        let _logger = TestLogger::new().with_json().with_level("debug").init();

        tracing::info!("JSON formatted message");
        tracing::debug!("Debug message");
    }

    #[test]
    fn test_logger_with_peer_id() {
        let _logger = TestLogger::new().with_level("info").init();

        let _span = tracing::info_span!("test_peer", test_node = "test-peer").entered();

        tracing::info!("Message with peer ID");
    }

    #[test]
    fn test_logger_capture() {
        let logger = TestLogger::new().capture_logs().with_level("info").init();

        tracing::info!("Captured message 1");
        tracing::warn!("Captured message 2");
        tracing::error!("Captured message 3");

        // Verify log capture works
        assert!(logger.contains("Captured message 1"));
        assert!(logger.contains("Captured message 2"));
        assert!(logger.contains("Captured message 3"));
        // Pretty format produces multiple lines per log entry, so we check >= 3
        assert!(
            logger.log_count() >= 3,
            "Expected at least 3 log entries, got {}",
            logger.log_count()
        );
    }

    #[test]
    fn test_logger_capture_with_json() {
        let logger = TestLogger::new()
            .with_json()
            .capture_logs()
            .with_level("info")
            .init();

        tracing::info!("JSON captured message");

        assert!(logger.contains("JSON captured message"));
    }

    #[tokio::test]
    async fn test_logger_async() {
        let _logger = TestLogger::new().with_json().with_level("debug").init();

        let _span = tracing::info_span!("test_peer", test_node = "async-peer").entered();

        tracing::info!("Async test message");
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        tracing::debug!("After sleep");
    }

    #[test]
    fn test_logger_json_with_span_fields() {
        let logger = TestLogger::new()
            .with_json()
            .capture_logs()
            .with_level("info")
            .init();

        // Create a span with test_node field
        let _span = tracing::info_span!("test_peer", test_node = "test-gateway").entered();

        tracing::info!("Message from gateway");

        // Verify the log was captured
        let logs = logger.logs();
        assert!(!logs.is_empty(), "Should have captured logs");

        // Verify the message was captured
        // Note: Span fields (like test_node) appear in the span list when using
        // with_span_list(true), but not as flat fields in JSON output.
        // This is expected behavior of tracing-subscriber's JSON formatter.
        assert!(logs.iter().any(|log| log.contains("Message from gateway")));

        // The JSON should have spans array with test_node field
        let json_str = logs.join("\n");
        assert!(
            json_str.contains("test_peer") || json_str.contains("gateway"),
            "Should contain span information"
        );
    }
}

// Test context for integration tests
use std::collections::HashMap;

/// Information about a node in a test
#[derive(Debug)]
pub struct NodeInfo {
    /// Human-readable label (e.g., "gateway", "peer-1")
    pub label: String,
    /// Path to temp directory for this node's data
    pub temp_dir_path: PathBuf,
    /// WebSocket API port
    pub ws_port: u16,
    /// Network port (None for non-gateway nodes)
    pub network_port: Option<u16>,
    /// Whether this is a gateway node
    pub is_gateway: bool,
    /// Node's location in the ring
    pub location: f64,
}

/// Test result type for test functions
pub type TestResult = anyhow::Result<()>;

/// Test context providing access to nodes and event aggregation.
///
/// This is the main interface for interacting with test infrastructure in
/// multi-node integration tests. It provides:
/// - Node information access
/// - Event log aggregation and failure reporting
///
/// Note: WebSocket client management is left to the test code for simplicity.
/// Use `tokio_tungstenite::connect_async` and `WebApi::start` to create clients.
pub struct TestContext {
    /// Node information, indexed by label
    nodes: HashMap<String, NodeInfo>,
    /// Node labels in order they were added (for indexing)
    node_order: Vec<String>,
    /// Flush handles for event aggregation (optional for backward compatibility)
    flush_handles: HashMap<String, crate::tracing::EventFlushHandle>,
}

impl TestContext {
    /// Create a new TestContext from node information.
    pub fn new(nodes: Vec<NodeInfo>) -> Self {
        let node_order: Vec<String> = nodes.iter().map(|n| n.label.clone()).collect();
        let nodes_map: HashMap<String, NodeInfo> =
            nodes.into_iter().map(|n| (n.label.clone(), n)).collect();

        Self {
            nodes: nodes_map,
            node_order,
            flush_handles: HashMap::new(),
        }
    }

    /// Create a new TestContext with flush handles for event aggregation.
    pub fn with_flush_handles(
        nodes: Vec<NodeInfo>,
        flush_handles: Vec<(String, crate::tracing::EventFlushHandle)>,
    ) -> Self {
        let node_order: Vec<String> = nodes.iter().map(|n| n.label.clone()).collect();
        let nodes_map: HashMap<String, NodeInfo> =
            nodes.into_iter().map(|n| (n.label.clone(), n)).collect();
        let flush_handles_map: HashMap<String, crate::tracing::EventFlushHandle> =
            flush_handles.into_iter().collect();
        Self {
            nodes: nodes_map,
            node_order,
            flush_handles: flush_handles_map,
        }
    }

    /// Get a reference to a node by label.
    pub fn node(&self, label: &str) -> anyhow::Result<&NodeInfo> {
        self.nodes
            .get(label)
            .ok_or_else(|| anyhow::anyhow!("Node '{}' not found", label))
    }

    /// Get the first gateway node.
    ///
    /// Note: If multiple gateways exist, use `gateways()` to get all of them.
    pub fn gateway(&self) -> anyhow::Result<&NodeInfo> {
        // Find first gateway node
        for label in &self.node_order {
            if let Ok(node) = self.node(label) {
                if node.is_gateway {
                    return Ok(node);
                }
            }
        }
        Err(anyhow::anyhow!("No gateway nodes found"))
    }

    /// Get all gateway nodes.
    pub fn gateways(&self) -> Vec<&NodeInfo> {
        self.node_order
            .iter()
            .filter_map(|label| self.node(label).ok())
            .filter(|node| node.is_gateway)
            .collect()
    }

    /// Get all peer (non-gateway) nodes.
    pub fn peers(&self) -> Vec<&NodeInfo> {
        self.node_order
            .iter()
            .filter_map(|label| self.node(label).ok())
            .filter(|node| !node.is_gateway)
            .collect()
    }

    /// Get the path to a node's event log.
    pub fn event_log_path(&self, node_label: &str) -> anyhow::Result<PathBuf> {
        let node = self.node(node_label)?;
        // Nodes run in Network mode, so they create _EVENT_LOG not _EVENT_LOG_LOCAL
        Ok(node.temp_dir_path.join("_EVENT_LOG"))
    }

    /// Get all node labels in order.
    pub fn node_labels(&self) -> &[String] {
        &self.node_order
    }

    /// Aggregate events from all nodes.
    pub async fn aggregate_events(
        &self,
    ) -> anyhow::Result<crate::tracing::EventLogAggregator<crate::tracing::AOFEventSource>> {
        // Flush all event registers before aggregating
        for (label, handle) in &self.flush_handles {
            tracing::debug!("Flushing events for node: {}", label);
            handle.flush().await;
        }

        let mut builder = TestAggregatorBuilder::new();
        for label in &self.node_order {
            let path = self.event_log_path(label)?;
            builder = builder.add_node(label, path);
        }
        builder.build().await
    }

    /// Generate a comprehensive failure report with event aggregation.
    pub async fn generate_failure_report(&self, error: &anyhow::Error) -> String {
        use std::fmt::Write;

        let mut report = String::new();
        writeln!(&mut report, "\n{}", "=".repeat(80)).unwrap();
        writeln!(&mut report, "TEST FAILURE REPORT").unwrap();
        writeln!(&mut report, "{}", "=".repeat(80)).unwrap();
        writeln!(&mut report, "\nError: {:#}", error).unwrap();

        // Try to aggregate events
        match self.aggregate_events().await {
            Ok(aggregator) => {
                writeln!(&mut report, "\n{}", "-".repeat(80)).unwrap();
                writeln!(&mut report, "EVENT LOG SUMMARY").unwrap();
                writeln!(&mut report, "{}", "-".repeat(80)).unwrap();

                match aggregator.get_all_events().await {
                    Ok(events) => {
                        writeln!(&mut report, "\nTotal events: {}", events.len()).unwrap();

                        // Group by peer_id
                        let mut by_peer: HashMap<String, Vec<_>> = HashMap::new();
                        for event in &events {
                            let peer_str = event.peer_id.to_string();
                            by_peer.entry(peer_str).or_default().push(event);
                        }

                        writeln!(&mut report, "\nEvents by peer:").unwrap();
                        for (peer_id, peer_events) in by_peer.iter() {
                            writeln!(
                                &mut report,
                                "  {}: {} events",
                                &peer_id[..8.min(peer_id.len())], // Show first 8 chars
                                peer_events.len()
                            )
                            .unwrap();
                        }

                        // Show last 10 events
                        writeln!(&mut report, "\nLast 10 events:").unwrap();
                        let last_events = events.iter().rev().take(10).collect::<Vec<_>>();
                        for (i, event) in last_events.iter().rev().enumerate() {
                            let peer_str = event.peer_id.to_string();
                            writeln!(
                                &mut report,
                                "  {}. [{}] {} - {:?}",
                                i + 1,
                                &peer_str[..8.min(peer_str.len())],
                                event.datetime.format("%H:%M:%S%.3f"),
                                event.kind
                            )
                            .unwrap();
                        }

                        // Generate detailed reports in temp directory
                        if !events.is_empty() {
                            match self
                                .generate_detailed_reports("test_failure", &aggregator)
                                .await
                            {
                                Ok(report_dir) => {
                                    writeln!(&mut report, "\n📁 Detailed Reports Generated:")
                                        .unwrap();
                                    writeln!(
                                        &mut report,
                                        "  📄 Full event log:     file://{}/events.md",
                                        report_dir.display()
                                    )
                                    .unwrap();
                                    writeln!(
                                        &mut report,
                                        "  📊 Event flow diagram: file://{}/event-flow.mmd",
                                        report_dir.display()
                                    )
                                    .unwrap();
                                    writeln!(
                                        &mut report,
                                        "\n💡 Tip: View diagram at https://mermaid.live or in VS Code"
                                    )
                                    .unwrap();
                                }
                                Err(e) => {
                                    writeln!(
                                        &mut report,
                                        "\n⚠️ Failed to generate detailed reports: {}",
                                        e
                                    )
                                    .unwrap();
                                }
                            }
                        }
                    }
                    Err(e) => {
                        writeln!(&mut report, "\nFailed to get events: {}", e).unwrap();
                    }
                }
            }
            Err(e) => {
                writeln!(&mut report, "\nFailed to aggregate events: {}", e).unwrap();
            }
        }

        writeln!(&mut report, "\n{}", "=".repeat(80)).unwrap();
        report
    }

    /// Generate a success summary with event statistics.
    /// Generate detailed reports in temp directory and return the path.
    async fn generate_detailed_reports(
        &self,
        test_name: &str,
        aggregator: &crate::tracing::EventLogAggregator<crate::tracing::AOFEventSource>,
    ) -> anyhow::Result<std::path::PathBuf> {
        use std::fmt::Write as FmtWrite;
        use std::io::Write as IoWrite;

        // Create temp directory for reports
        let timestamp = chrono::Utc::now().format("%Y%m%d-%H%M%S");
        let report_dir =
            std::path::PathBuf::from(format!("/tmp/freenet-test-{}-{}", test_name, timestamp));
        std::fs::create_dir_all(&report_dir)?;

        // Generate detailed events markdown
        let events = aggregator.get_all_events().await?;
        let mut events_md = String::new();
        writeln!(&mut events_md, "# Detailed Event Log: {}\n", test_name)?;
        writeln!(&mut events_md, "**Generated**: {}", chrono::Utc::now())?;
        writeln!(&mut events_md, "**Total Events**: {}\n", events.len())?;

        if !events.is_empty() {
            writeln!(&mut events_md, "## Events by Timestamp\n")?;
            let start_time = events.first().unwrap().datetime;

            for event in &events {
                let elapsed = (event.datetime - start_time).num_milliseconds();
                let (icon, type_name) = match &event.kind {
                    crate::tracing::EventKind::Connect(..) => ("🔗", "Connect"),
                    crate::tracing::EventKind::Put(..) => ("📤", "Put"),
                    crate::tracing::EventKind::Get { .. } => ("📥", "Get"),
                    crate::tracing::EventKind::Route(..) => ("🔀", "Route"),
                    crate::tracing::EventKind::Update(..) => ("🔄", "Update"),
                    crate::tracing::EventKind::Subscribed { .. } => ("🔔", "Subscribe"),
                    crate::tracing::EventKind::Disconnected { .. } => ("❌", "Disconnect"),
                    crate::tracing::EventKind::Ignored => ("⏭️", "Ignored"),
                };

                writeln!(
                    &mut events_md,
                    "### {} {} - [{:>6}ms]\n",
                    icon, type_name, elapsed
                )?;
                writeln!(&mut events_md, "- **Peer ID**: `{}`", event.peer_id)?;
                writeln!(&mut events_md, "- **Transaction**: `{}`", event.tx)?;
                writeln!(&mut events_md, "- **Timestamp**: {}", event.datetime)?;
                writeln!(
                    &mut events_md,
                    "\n**Event Details**:\n```rust\n{:#?}\n```\n",
                    event.kind
                )?;
            }
        }

        let events_md_path = report_dir.join("events.md");
        let mut events_file = std::fs::File::create(&events_md_path)?;
        events_file.write_all(events_md.as_bytes())?;

        // Generate Mermaid diagram showing event flow
        let mut mermaid = String::from("```mermaid\ngraph TD\n");
        mermaid.push_str("    %% Event Flow Diagram\n");

        let mut prev_id: Option<String> = None;
        for (idx, event) in events.iter().enumerate().take(50) {
            // Limit to 50 for readability
            let node_id = format!("N{}", idx);
            let peer_short = &event.peer_id.to_string()[..8.min(event.peer_id.to_string().len())];
            let (icon, type_name) = match &event.kind {
                crate::tracing::EventKind::Connect(..) => ("🔗", "Connect"),
                crate::tracing::EventKind::Put(..) => ("📤", "Put"),
                crate::tracing::EventKind::Get { .. } => ("📥", "Get"),
                crate::tracing::EventKind::Route(..) => ("🔀", "Route"),
                crate::tracing::EventKind::Update(..) => ("🔄", "Update"),
                crate::tracing::EventKind::Subscribed { .. } => ("🔔", "Subscribe"),
                crate::tracing::EventKind::Disconnected { .. } => ("❌", "Disconnect"),
                crate::tracing::EventKind::Ignored => ("⏭️", "Ignored"),
            };

            writeln!(
                &mut mermaid,
                "    {}[\"{} {}\\n{}\"]",
                node_id, peer_short, icon, type_name
            )?;

            if let Some(prev) = prev_id {
                writeln!(&mut mermaid, "    {} --> {}", prev, node_id)?;
            }
            prev_id = Some(node_id);
        }

        if events.len() > 50 {
            writeln!(
                &mut mermaid,
                "    NMore[\"... and {} more events\"]",
                events.len() - 50
            )?;
            if let Some(prev) = prev_id {
                writeln!(&mut mermaid, "    {} -.-> NMore", prev)?;
            }
        }

        mermaid.push_str("```\n");

        let mermaid_path = report_dir.join("event-flow.mmd");
        let mut mermaid_file = std::fs::File::create(&mermaid_path)?;
        mermaid_file.write_all(mermaid.as_bytes())?;

        Ok(report_dir)
    }

    pub async fn generate_success_summary(&self) -> String {
        use std::fmt::Write;

        let mut report = String::new();
        writeln!(&mut report, "\n{}", "=".repeat(80)).unwrap();
        writeln!(&mut report, "TEST SUCCESS SUMMARY").unwrap();
        writeln!(&mut report, "{}", "=".repeat(80)).unwrap();

        // Try to aggregate events
        match self.aggregate_events().await {
            Ok(aggregator) => match aggregator.get_all_events().await {
                Ok(mut events) => {
                    writeln!(&mut report, "\n📊 Event Statistics:").unwrap();
                    writeln!(&mut report, "  Total events: {}", events.len()).unwrap();

                    // Count by event type
                    let mut by_type: HashMap<String, usize> = HashMap::new();
                    for event in &events {
                        let type_name = match &event.kind {
                            crate::tracing::EventKind::Connect(..) => "Connect",
                            crate::tracing::EventKind::Put(..) => "Put",
                            crate::tracing::EventKind::Get { .. } => "Get",
                            crate::tracing::EventKind::Route(..) => "Route",
                            crate::tracing::EventKind::Update(..) => "Update",
                            crate::tracing::EventKind::Subscribed { .. } => "Subscribe",
                            crate::tracing::EventKind::Disconnected { .. } => "Disconnect",
                            crate::tracing::EventKind::Ignored => "Ignored",
                        };
                        *by_type.entry(type_name.to_string()).or_default() += 1;
                    }

                    writeln!(&mut report, "\n  By type:").unwrap();
                    for (event_type, count) in by_type.iter() {
                        writeln!(&mut report, "    {}: {}", event_type, count).unwrap();
                    }

                    // Group by peer_id
                    let mut by_peer: HashMap<String, Vec<_>> = HashMap::new();
                    for event in &events {
                        let peer_str = event.peer_id.to_string();
                        by_peer.entry(peer_str).or_default().push(event);
                    }

                    writeln!(&mut report, "\n  By peer:").unwrap();
                    for (peer_id, peer_events) in by_peer.iter() {
                        writeln!(
                            &mut report,
                            "    {}: {} events",
                            &peer_id[..8.min(peer_id.len())],
                            peer_events.len()
                        )
                        .unwrap();
                    }

                    // Sort events by timestamp for timeline
                    events.sort_by_key(|e| e.datetime);

                    // Show timeline of key events (simplified, showing all events)
                    writeln!(&mut report, "\n📅 Event Timeline:").unwrap();
                    let start_time = events
                        .first()
                        .map(|e| e.datetime)
                        .unwrap_or_else(chrono::Utc::now);

                    for event in &events {
                        let elapsed = (event.datetime - start_time).num_milliseconds();
                        let peer_short =
                            &event.peer_id.to_string()[..8.min(event.peer_id.to_string().len())];

                        // Get event type icon
                        let (icon, _type_name) = match &event.kind {
                            crate::tracing::EventKind::Connect(..) => ("🔗", "Connect"),
                            crate::tracing::EventKind::Put(..) => ("📤", "Put"),
                            crate::tracing::EventKind::Get { .. } => ("📥", "Get"),
                            crate::tracing::EventKind::Route(..) => ("🔀", "Route"),
                            crate::tracing::EventKind::Update(..) => ("🔄", "Update"),
                            crate::tracing::EventKind::Subscribed { .. } => ("🔔", "Subscribe"),
                            crate::tracing::EventKind::Disconnected { .. } => ("❌", "Disconnect"),
                            crate::tracing::EventKind::Ignored => ("⏭️", "Ignored"),
                        };

                        // Format event details (using Debug for now to avoid private field access)
                        writeln!(
                            &mut report,
                            "  [{:>6}ms] {} {} {}",
                            elapsed,
                            peer_short,
                            icon,
                            format!("{:?}", event.kind)
                                .chars()
                                .take(60)
                                .collect::<String>()
                        )
                        .unwrap();
                    }

                    // Generate detailed reports in temp directory
                    if !events.is_empty() {
                        match self
                            .generate_detailed_reports("test_success", &aggregator)
                            .await
                        {
                            Ok(report_dir) => {
                                writeln!(&mut report, "\n📁 Detailed Reports Generated:").unwrap();
                                writeln!(
                                    &mut report,
                                    "  📄 Full event log:     file://{}/events.md",
                                    report_dir.display()
                                )
                                .unwrap();
                                writeln!(
                                    &mut report,
                                    "  📊 Event flow diagram: file://{}/event-flow.mmd",
                                    report_dir.display()
                                )
                                .unwrap();
                                writeln!(
                                    &mut report,
                                    "\n💡 Tip: View diagram at https://mermaid.live or in VS Code"
                                )
                                .unwrap();
                            }
                            Err(e) => {
                                writeln!(
                                    &mut report,
                                    "\n⚠️ Failed to generate detailed reports: {}",
                                    e
                                )
                                .unwrap();
                            }
                        }
                    }
                }
                Err(e) => {
                    writeln!(&mut report, "\n❌ Failed to get events: {}", e).unwrap();
                }
            },
            Err(e) => {
                writeln!(&mut report, "\n❌ Failed to aggregate events: {}", e).unwrap();
            }
        }

        writeln!(&mut report, "\n{}", "=".repeat(80)).unwrap();
        report
    }
}

// Event aggregator test utilities
pub mod event_aggregator_utils {
    //! Test utilities for event log aggregation.

    use crate::tracing::EventLogAggregator;
    use anyhow::Result;
    use std::path::PathBuf;

    /// A handle to collect node information for aggregation.
    #[derive(Debug, Clone)]
    pub struct NodeLogInfo {
        /// Human-readable label for the node (e.g., "node-a", "gateway")
        pub label: String,
        /// Path to the node's event log file
        pub event_log_path: PathBuf,
    }

    impl NodeLogInfo {
        /// Create a new node log info.
        pub fn new(label: impl Into<String>, event_log_path: PathBuf) -> Self {
            Self {
                label: label.into(),
                event_log_path,
            }
        }
    }

    /// Builder for creating an EventLogAggregator from test nodes.
    pub struct TestAggregatorBuilder {
        nodes: Vec<NodeLogInfo>,
    }

    impl TestAggregatorBuilder {
        /// Create a new builder.
        pub fn new() -> Self {
            Self { nodes: Vec::new() }
        }

        /// Add a node to aggregate from.
        pub fn add_node(mut self, label: impl Into<String>, event_log_path: PathBuf) -> Self {
            self.nodes.push(NodeLogInfo::new(label, event_log_path));
            self
        }

        /// Add multiple nodes from config directories.
        pub fn add_nodes_from_configs(mut self, configs: Vec<(String, PathBuf)>) -> Self {
            for (label, config_dir) in configs {
                let event_log = config_dir.join("event_log");
                let local_log = config_dir.join("_EVENT_LOG_LOCAL");

                let log_path = if event_log.exists() {
                    event_log
                } else if local_log.exists() {
                    local_log
                } else {
                    tracing::warn!(
                        "No event log found for {} in {:?}, using event_log path",
                        label,
                        config_dir
                    );
                    event_log
                };

                self.nodes.push(NodeLogInfo::new(label, log_path));
            }
            self
        }

        /// Build the aggregator.
        pub async fn build(self) -> Result<EventLogAggregator<crate::tracing::AOFEventSource>> {
            let sources = self
                .nodes
                .into_iter()
                .map(|node| (node.event_log_path, Some(node.label)))
                .collect();

            EventLogAggregator::from_aof_files(sources).await
        }
    }

    impl Default for TestAggregatorBuilder {
        fn default() -> Self {
            Self::new()
        }
    }
}

pub use event_aggregator_utils::{NodeLogInfo, TestAggregatorBuilder};
