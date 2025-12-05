//! Event Log Analysis Example
//!
//! This example demonstrates how to use the Event Log Aggregator to analyze
//! distributed operations across multiple Freenet nodes.
//!
//! # Usage
//!
//! ```bash
//! # After running integration tests that generate event logs:
//! cargo run --example analyze_event_logs -- \
//!     --gateway /tmp/gateway/_EVENT_LOG_LOCAL \
//!     --peer /tmp/node-a/_EVENT_LOG_LOCAL
//! ```
//!
//! Or provide event log directories:
//! ```bash
//! cargo run --example analyze_event_logs --paths /tmp/test1 /tmp/test2
//! ```

use anyhow::Result;
use freenet::tracing::{AOFEventSource, EventLogAggregator};
use std::collections::HashMap;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 || args.contains(&"--help".to_string()) {
        print_usage();
        return Ok(());
    }

    let log_paths = parse_args(&args)?;

    if log_paths.is_empty() {
        eprintln!("❌ No event log files found!");
        eprintln!("   Please provide paths to event log files or directories.");
        print_usage();
        return Ok(());
    }

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║          Freenet Event Log Analyzer                         ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Create aggregator
    println!("📂 Loading event logs...\n");
    for (i, (path, label)) in log_paths.iter().enumerate() {
        println!(
            "   {}. {} → {:?}",
            i + 1,
            label.as_ref().unwrap_or(&"unknown".to_string()),
            path
        );
    }

    let aggregator = EventLogAggregator::<AOFEventSource>::from_aof_files(log_paths).await?;

    // Get all events
    let events = aggregator.get_all_events().await?;

    if events.is_empty() {
        println!("\n⚠️  No events found in the provided logs.");
        println!("   Event logs may be empty or nodes haven't logged any operations yet.\n");
        return Ok(());
    }

    println!("\n✅ Loaded {} events from all nodes\n", events.len());

    // Analyze events
    println!("═══════════════════════════════════════════════════════════════");
    println!("                    EVENT SUMMARY");
    println!("═══════════════════════════════════════════════════════════════\n");

    // Count events by type
    let mut event_types: HashMap<String, usize> = HashMap::new();
    let mut nodes: HashMap<String, usize> = HashMap::new();
    let mut transactions = std::collections::HashSet::new();

    for event in &events {
        let event_type = format!("{:?}", event.kind)
            .split('(')
            .next()
            .unwrap_or("Unknown")
            .to_string();
        *event_types.entry(event_type).or_insert(0) += 1;

        let node_id = format!("{:.8}", event.peer_id.to_string());
        *nodes.entry(node_id).or_insert(0) += 1;

        transactions.insert(event.tx);
    }

    println!("📊 Event Types:");
    let mut sorted_types: Vec<_> = event_types.iter().collect();
    sorted_types.sort_by(|a, b| b.1.cmp(a.1));
    for (event_type, count) in sorted_types {
        println!("   • {:<20} {: >5} events", event_type, count);
    }

    println!("\n🖥️  Nodes Participating:");
    for (node, count) in &nodes {
        println!("   • {:<20} {: >5} events", node, count);
    }

    println!("\n🔄 Unique Transactions: {}", transactions.len());

    // Show timeline
    if let (Some(first), Some(last)) = (events.first(), events.last()) {
        let duration = last.datetime.signed_duration_since(first.datetime);
        println!("⏱️  Timeline:");
        println!(
            "   Start: {}",
            first.datetime.format("%Y-%m-%d %H:%M:%S%.3f")
        );
        println!(
            "   End:   {}",
            last.datetime.format("%Y-%m-%d %H:%M:%S%.3f")
        );
        println!(
            "   Duration: {:.2}s",
            duration.num_milliseconds() as f64 / 1000.0
        );
    }

    // Analyze a specific transaction if we have any
    if let Some(&tx_id) = transactions.iter().next() {
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("              TRANSACTION FLOW ANALYSIS");
        println!("═══════════════════════════════════════════════════════════════\n");
        println!("Analyzing transaction: {}\n", tx_id);

        let flow = aggregator.get_transaction_flow(&tx_id).await?;

        println!("📍 Transaction Path ({} hops):\n", flow.len());
        for (i, event) in flow.iter().enumerate() {
            let timestamp = event.timestamp.format("%H:%M:%S%.3f");
            let node_label = event.peer_label.as_deref().unwrap_or("unknown");
            let event_kind = format!("{:?}", event.event_kind);

            println!(
                "   {}. [{}] {} on {}",
                i + 1,
                timestamp,
                event_kind.split('(').next().unwrap_or(&event_kind),
                node_label
            );
        }

        // Try to get routing path
        if let Ok(routing) = aggregator.get_routing_path(&tx_id).await {
            println!("\n🗺️  Routing Path:");
            for (peer_id, label) in &routing.path {
                if let Some(l) = label {
                    println!("   → {}", l);
                } else {
                    println!("   → {:.8}", peer_id);
                }
            }
            if let Some(duration) = routing.duration {
                println!(
                    "\n   Total Duration: {:.3}s",
                    duration.num_milliseconds() as f64 / 1000.0
                );
            }
        }

        // Generate Mermaid diagram
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("              VISUAL FLOW DIAGRAM");
        println!("═══════════════════════════════════════════════════════════════\n");
        println!("Copy the following to visualize at https://mermaid.live\n");

        let diagram = aggregator.export_mermaid_graph(&tx_id).await?;
        println!("{}", diagram);
    }

    println!("\n═══════════════════════════════════════════════════════════════");
    println!("                    ANALYSIS COMPLETE");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("💡 Tips:");
    println!("   • Use --help to see all options");
    println!("   • Event logs are written to _EVENT_LOG_LOCAL in node config dirs");
    println!("   • Run integration tests to generate event logs for analysis");
    println!("\n");

    Ok(())
}

fn parse_args(args: &[String]) -> Result<Vec<(PathBuf, Option<String>)>> {
    let mut log_paths = Vec::new();
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--gateway" | "-g" => {
                if i + 1 < args.len() {
                    log_paths.push((PathBuf::from(&args[i + 1]), Some("gateway".to_string())));
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--peer" | "-p" => {
                if i + 1 < args.len() {
                    let label = if i + 2 < args.len() && !args[i + 2].starts_with("--") {
                        Some(args[i + 2].clone())
                    } else {
                        Some(format!("peer-{}", log_paths.len()))
                    };
                    let is_auto_label = label
                        .as_ref()
                        .map(|l| l.starts_with("peer-"))
                        .unwrap_or(true);
                    log_paths.push((PathBuf::from(&args[i + 1]), label));
                    i += if is_auto_label { 2 } else { 3 };
                } else {
                    i += 1;
                }
            }
            "--paths" => {
                i += 1;
                while i < args.len() && !args[i].starts_with("--") {
                    let path = PathBuf::from(&args[i]);
                    // Check if it's a directory containing event logs
                    if path.is_dir() {
                        let event_log = path.join("_EVENT_LOG_LOCAL");
                        if event_log.exists() {
                            let label = path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .map(|s| s.to_string());
                            log_paths.push((event_log, label));
                        }
                    } else if path.exists() {
                        log_paths.push((path, None));
                    }
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(log_paths)
}

fn print_usage() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║          Freenet Event Log Analyzer                         ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");
    println!("USAGE:");
    println!("    cargo run --example analyze_event_logs [OPTIONS]\n");
    println!("OPTIONS:");
    println!("    --gateway, -g <PATH>      Path to gateway event log file");
    println!("    --peer, -p <PATH> [NAME]  Path to peer event log file (optional name)");
    println!("    --paths <DIR1> <DIR2>...  Paths to node directories (auto-finds logs)");
    println!("    --help                     Show this help message\n");
    println!("EXAMPLES:");
    println!("    # Analyze specific log files");
    println!("    cargo run --example analyze_event_logs \\");
    println!("        --gateway /tmp/gateway/_EVENT_LOG_LOCAL \\");
    println!("        --peer /tmp/node-a/_EVENT_LOG_LOCAL node-a\n");
    println!("    # Auto-discover logs in directories");
    println!("    cargo run --example analyze_event_logs \\");
    println!("        --paths /tmp/test_run_*/\n");
    println!("OUTPUT:");
    println!("    • Event statistics and summaries");
    println!("    • Transaction flow analysis");
    println!("    • Routing path visualization");
    println!("    • Mermaid diagram for visual flow\n");
}
