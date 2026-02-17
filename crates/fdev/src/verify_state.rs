//! State verification subcommand for fdev.
//!
//! Reads event log (AOF) files from one or more nodes and runs the state
//! verifier to detect consistency anomalies.
//!
//! # Usage
//!
//! ```text
//! # Verify state from individual node event logs
//! fdev verify-state --log-file /tmp/node-a/_EVENT_LOG_LOCAL \
//!                   --log-file /tmp/node-b/_EVENT_LOG_LOCAL
//!
//! # Verify state from a log directory (auto-discovers event logs)
//! fdev verify-state --log-directory /tmp/freenet-sim-logs/
//! ```

use std::path::PathBuf;

/// Configuration for the verify-state subcommand.
#[derive(clap::Parser, Clone)]
pub struct VerifyStateConfig {
    /// Paths to individual event log (AOF) files from nodes.
    /// Specify multiple times for multiple nodes.
    #[arg(long = "log-file", short = 'f')]
    pub log_files: Vec<PathBuf>,

    /// Directory containing event log files. All files named
    /// `_EVENT_LOG_LOCAL` found recursively will be included.
    #[arg(long = "log-directory", short = 'd')]
    pub log_directory: Option<PathBuf>,

    /// Only report anomalies of this type (missing-broadcast, unapplied-broadcast,
    /// unexpected-state-change, final-divergence). By default, all types are shown.
    #[arg(long = "filter")]
    pub filter: Option<AnomalyFilter>,

    /// Output the full per-contract state history timeline.
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum AnomalyFilter {
    MissingBroadcast,
    UnappliedBroadcast,
    UnexpectedStateChange,
    FinalDivergence,
}

pub async fn verify_state(config: VerifyStateConfig) -> anyhow::Result<()> {
    let mut paths: Vec<(PathBuf, Option<String>)> = Vec::new();

    // Collect explicitly specified log files
    for (i, path) in config.log_files.iter().enumerate() {
        if !path.exists() {
            eprintln!("Warning: log file does not exist: {}", path.display());
            continue;
        }
        let label = path
            .parent()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("node-{}", i));
        paths.push((path.clone(), Some(label)));
    }

    // Discover log files in directory
    if let Some(dir) = &config.log_directory {
        if !dir.exists() {
            anyhow::bail!("Log directory does not exist: {}", dir.display());
        }
        discover_event_logs(dir, &mut paths)?;
    }

    if paths.is_empty() {
        anyhow::bail!("No event log files found. Specify --log-file or --log-directory.");
    }

    println!("Loading event logs from {} source(s)...", paths.len());
    for (path, label) in &paths {
        println!(
            "  {} ({})",
            label.as_deref().unwrap_or("unknown"),
            path.display()
        );
    }

    let verifier = freenet::tracing::StateVerifier::from_aof_paths(paths).await?;
    let report = verifier.verify();

    // Apply filter if specified
    let anomalies: Vec<_> = if let Some(filter) = &config.filter {
        report
            .anomalies
            .iter()
            .filter(|a| matches_filter(a, filter))
            .collect()
    } else {
        report.anomalies.iter().collect()
    };

    // Print summary
    println!("\n=== State Verification Report ===");
    println!(
        "Events analyzed: {} ({} state-mutating)",
        report.total_events, report.state_events
    );
    println!("Contracts: {}", report.contracts_analyzed);
    println!(
        "Anomalies: {} (showing: {})",
        report.anomalies.len(),
        anomalies.len()
    );

    if anomalies.is_empty() {
        println!("\nNo anomalies detected. All contracts converged.");
    } else {
        println!();
        for (i, anomaly) in anomalies.iter().enumerate() {
            println!("  [{}] {}", i + 1, anomaly);
        }
    }

    // Verbose: print per-contract timeline
    if config.verbose {
        println!("\n=== Contract State Histories ===");
        for history in &report.contract_histories {
            println!("\n--- Contract: {} ---", history.contract_key);
            println!(
                "Transitions: {}, Broadcasts emitted: {}",
                history.transitions.len(),
                history.emitted_broadcasts.len()
            );

            for transition in &history.transitions {
                println!(
                    "  [{}] peer={} tx={} kind={} {} -> {}",
                    transition.timestamp.format("%H:%M:%S%.3f"),
                    transition.peer,
                    transition.transaction,
                    transition.kind,
                    transition.state_before.as_deref().unwrap_or("(none)"),
                    transition.state_after
                );
            }

            println!("Final states:");
            for (peer, hash) in &history.final_peer_states {
                println!("  {} = {}", peer, hash);
            }
        }
    }

    // Exit with non-zero status if anomalies found
    if !report.is_clean() {
        std::process::exit(1);
    }

    Ok(())
}

fn discover_event_logs(
    dir: &PathBuf,
    paths: &mut Vec<(PathBuf, Option<String>)>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            // Check for _EVENT_LOG_LOCAL inside this directory
            let log_path = path.join("_EVENT_LOG_LOCAL");
            if log_path.exists() {
                let label = path.file_name().map(|n| n.to_string_lossy().to_string());
                paths.push((log_path, label));
            } else {
                // Recurse one more level
                discover_event_logs(&path, paths)?;
            }
        } else if path
            .file_name()
            .map(|n| n.to_string_lossy().contains("EVENT_LOG"))
            .unwrap_or(false)
        {
            let label = path
                .parent()
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().to_string());
            paths.push((path, label));
        }
    }
    Ok(())
}

fn matches_filter(
    anomaly: &freenet::tracing::state_verifier::StateAnomaly,
    filter: &AnomalyFilter,
) -> bool {
    use freenet::tracing::state_verifier::StateAnomaly;
    match filter {
        AnomalyFilter::MissingBroadcast => {
            matches!(anomaly, StateAnomaly::MissingBroadcast { .. })
        }
        AnomalyFilter::UnappliedBroadcast => {
            matches!(anomaly, StateAnomaly::BroadcastNotApplied { .. })
        }
        AnomalyFilter::UnexpectedStateChange => {
            matches!(anomaly, StateAnomaly::UnexpectedStateChange { .. })
        }
        AnomalyFilter::FinalDivergence => {
            matches!(anomaly, StateAnomaly::FinalDivergence { .. })
        }
    }
}
