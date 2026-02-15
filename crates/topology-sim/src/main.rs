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
    let scales = [300, 3_000];

    for &n in &scales {
        println!("\n{}", "=".repeat(60));
        println!("  SCALE: {} peers", n);
        println!("{}", "=".repeat(60));

        let strategies: Vec<(&str, Strategy)> = vec![
            ("current", Strategy::Current(strategy::Current)),
            ("small_world", Strategy::SmallWorld(strategy::SmallWorld)),
        ];

        for (name, strat) in strategies {
            println!("\n--- Strategy: {} ---", name);
            let mut net = Network::new(n, 10, 20, strat);

            // Run simulation: 500 ticks of connection formation + maintenance
            for _ in 0..500 {
                net.tick();
            }

            let m = metrics::compute(&net);
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
                    "    [{:.2}-{:.2}): {:5.1}% {}",
                    bin.lo, bin.hi, bin.pct, bar
                );
            }
        }
    }
}
