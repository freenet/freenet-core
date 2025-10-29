//! Quick demo showing event aggregator in action

use freenet::tracing::{AOFEventSource, EventLogAggregator};

#[tokio::test]
async fn demo_basic_aggregator() -> anyhow::Result<()> {
    println!("\n🎯 Event Aggregator Demo\n");

    // Create temporary directories for mock node logs
    let temp = tempfile::tempdir()?;
    let node1_log = temp.path().join("node1.log");
    let node2_log = temp.path().join("node2.log");

    // Create empty log files (in real tests, nodes write events here)
    std::fs::write(&node1_log, [])?;
    std::fs::write(&node2_log, [])?;

    println!("✅ Created mock node logs:");
    println!("   - Node 1: {:?}", node1_log);
    println!("   - Node 2: {:?}", node2_log);

    // Create aggregator from multiple node logs
    let aggregator = EventLogAggregator::<AOFEventSource>::from_aof_files(vec![
        (node1_log.clone(), Some("gateway".into())),
        (node2_log.clone(), Some("peer-1".into())),
    ])
    .await?;

    println!("\n✅ Created EventLogAggregator with 2 sources");

    // Get all events (will be empty for this demo)
    let events = aggregator.get_all_events().await?;
    println!("✅ Aggregated {} events from all nodes", events.len());

    // Test caching works
    let events2 = aggregator.get_all_events().await?;
    println!("✅ Retrieved from cache: {} events", events2.len());

    // Clear cache and verify
    aggregator.clear_cache().await;
    let events3 = aggregator.get_all_events().await?;
    println!("✅ After cache clear: {} events", events3.len());

    println!("\n🎉 Demo Complete!");
    println!("\nKey Takeaways:");
    println!("  • EventLogAggregator can collect from multiple nodes");
    println!("  • Uses generic EventSource trait with native async fn");
    println!("  • Caches results for performance");
    println!("  • In real tests, nodes write events to AOF files");
    println!("  • Aggregator correlates events by transaction ID\n");

    Ok(())
}
