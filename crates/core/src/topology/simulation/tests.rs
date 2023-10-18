

fn setup() {
    // Initialize the tracer
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG) // Adjust the level here
        .try_init()
        .unwrap_or(());
}

#[test]
fn it_ticks_the_network() {
    let mut network = SimulatedNetwork::new();
    network.tick();
    assert_eq!(network.current_time, 1);
}

#[test]
fn test_add_node() {
    setup();
    let mut net = SimulatedNetwork::new();
    let node_ref = net.add_node();
    assert_eq!(node_ref.index, 0);
    assert_eq!(net.nodes.len(), 1);
}

#[test]
fn test_connect() {
    setup();
    let mut net = SimulatedNetwork::new();
    let a = net.add_node();
    let b = net.add_node();
    net.connections.connect(a, b);
    assert!(net.connections.0.get(&a).unwrap().contains(&b));
    assert!(net.connections.0.get(&b).unwrap().contains(&a));
}

#[test]
fn test_disconnect() {
    setup();
    let mut net = SimulatedNetwork::new();
    let a = net.add_node();
    let b = net.add_node();
    net.connections.connect(a, b);
    net.connections.disconnect(a, b);
    assert!(!net.connections.0.get(&a).unwrap().contains(&b));
    assert!(!net.connections.0.get(&b).unwrap().contains(&a));
}

#[test]
fn test_route_success() {
    setup();
    let mut net = SimulatedNetwork::new();
    let a = net.add_node();
    let b = net.add_node();
    net.connections.connect(a, b);

    // Mock a destination location that's the same as node b's location
    let destination = net.nodes[b.index].location;

    let result = net.route(&a, destination);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), vec![b]);
}

#[test]
fn test_route_loop_error() {
    setup();

    let mut net = SimulatedNetwork::new();
    let a = net.add_node();
    let b = net.add_node();
    let c = net.add_node();
    net.connect(a, b);
    net.connect(b, c);
    net.connect(c, a);

    // Mock a destination location that's not reachable
    let destination = Location::random(); // Assume random will not match any node

    let result = net.route(&a, destination);
    assert!(matches!(result, Err(RouteError::Loop)));
}

#[test]
fn test_route_dead_end() {
    setup();
    let mut net = SimulatedNetwork::new();
    let a = net.add_node();
    let destination = net.nodes[a.index].location;

    let result = net.route(&a, destination);
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty()); // Since there's no closer node
}
#[test]
fn test_join_success() {
    let mut net = SimulatedNetwork::new();
    let a = net.add_node();
    let b = net.add_node();
    let c = net.add_node();
    net.connect(a, b);
    net.connect(b, c);

    let destination = net.nodes[c.index].location;
    let tolerance = Distance::new(0.4); // Changed to a valid value

    let join_result = net.get_join_peers(&a, destination, tolerance);

    assert!(join_result.is_some());
    assert_eq!(join_result.unwrap(), vec![b, c]);
}

#[test]
fn test_join_failure() {
    let mut net = SimulatedNetwork::new();
    let a = net.add_node();

    let destination = Location::random();
    let tolerance = Distance::new(0.4); // Changed to a valid value

    let join_result = net.get_join_peers(&a, destination, tolerance);

    assert!(join_result.is_none());
}

#[test]
fn test_join_with_tolerance() {
    let mut net = SimulatedNetwork::new();
    let a = net.add_node();
    let b = net.add_node();
    net.connect(a, b);

    let destination = net.nodes[b.index].location;
    let tolerance = Distance::new(0.1); 

    let join_result = net.get_join_peers(&a, destination, tolerance);

    assert!(join_result.is_some());
    assert_eq!(join_result.unwrap(), vec![b]);
}

