//! Topology simulation for evaluating Freenet small-world network formation strategies.
//!
//! Simulates peers on a ring [0, 1) forming connections using different strategies,
//! then measures the resulting topology against small-world ideals.

mod metrics;
mod network;
mod strategy;

use network::Network;
use strategy::Strategy;

fn main() {
    // Seed for reproducible results. Override with first CLI argument.
    let seed: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(42);

    let scales: Vec<(usize, usize, usize)> = vec![
        // (num_peers, min_connections, max_connections)
        (300, 10, 20),
        (3_000, 10, 20),
        (30_000, 15, 30),
        (100_000, 15, 30),
    ];

    for &(n, min_conn, max_conn) in &scales {
        println!("\n{}", "=".repeat(60));
        println!(
            "  SCALE: {} peers  (connections: {}-{})  seed: {}",
            n, min_conn, max_conn, seed
        );
        println!("{}", "=".repeat(60));

        let strategies: Vec<(&str, Strategy)> = vec![
            ("current", Strategy::Current(strategy::Current)),
            ("small_world", Strategy::SmallWorld(strategy::SmallWorld)),
        ];

        for (name, strat) in strategies {
            let start = std::time::Instant::now();
            let mut net = Network::new(n, min_conn, max_conn, strat, seed);

            let ticks = if n <= 1000 { 500 } else { 1000 };
            for _ in 0..ticks {
                net.tick();
            }

            let elapsed = start.elapsed();
            println!(
                "\n--- Strategy: {} ({:.1}s) ---",
                name,
                elapsed.as_secs_f64()
            );

            let m = metrics::compute(&net, seed);
            println!("  Connections:  {}", m.total_connections);
            println!("  Avg degree:   {:.1}", m.avg_degree);
            println!("  Avg distance: {:.4}", m.avg_distance);
            println!(
                "  Short (<0.1): {:.1}%  Long (>0.3): {:.1}%",
                m.short_pct, m.long_pct
            );
            println!("  Nearest-3 connectivity: {:.1}%", m.nearest_3_pct);
            println!(
                "  Greedy routing success:  {:.1}% (avg hops: {:.1})",
                m.greedy_success_pct, m.avg_greedy_hops
            );
            println!("  Distance distribution:");
            for bin in &m.histogram {
                let bar: String = std::iter::repeat_n('#', (bin.pct * 2.0) as usize).collect();
                println!(
                    "    [{:.02}-{:.02}): {:5.1}% {}",
                    bin.lo, bin.hi, bin.pct, bar
                );
            }
        }
    }
}
