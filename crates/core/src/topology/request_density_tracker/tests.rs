use super::*;

#[test]
fn test_create_density_map() {
    let mut sw = RequestDensityTracker::new(5);
    sw.sample(Location::new(0.21));
    sw.sample(Location::new(0.22));
    sw.sample(Location::new(0.23));
    sw.sample(Location::new(0.61));
    sw.sample(Location::new(0.62));

    let mut neighbors = BTreeMap::new();
    neighbors.insert(Location::new(0.2), 1);
    neighbors.insert(Location::new(0.6), 1);

    let result = sw.create_density_map(&neighbors);
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.neighbor_request_counts.get(&Location::new(0.2)), Some(&3));
    assert_eq!(result.neighbor_request_counts.get(&Location::new(0.6)), Some(&2));
}

#[test]
fn test_wrap_around() {
    let mut sw = RequestDensityTracker::new(5);
    sw.sample(Location::new(0.21));
    sw.sample(Location::new(0.22));
    sw.sample(Location::new(0.23));
    sw.sample(Location::new(0.61));
    sw.sample(Location::new(0.62));

    let mut neighbors = BTreeMap::new();
    neighbors.insert(Location::new(0.6), 1);
    neighbors.insert(Location::new(0.9), 1);

    let result = sw.create_density_map(&neighbors);
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.neighbor_request_counts.get(&Location::new(0.9)), Some(&3));
    assert_eq!(result.neighbor_request_counts.get(&Location::new(0.6)), Some(&2));
}

#[test]
fn test_interpolate() {
    let mut sw = RequestDensityTracker::new(10);
    sw.sample(Location::new(0.19));
    sw.sample(Location::new(0.20));
    sw.sample(Location::new(0.21));
    sw.sample(Location::new(0.59));
    sw.sample(Location::new(0.60));

    let mut neighbors = BTreeMap::new();
    neighbors.insert(Location::new(0.2), 1);
    neighbors.insert(Location::new(0.6), 1);

    let result = sw.create_density_map(&neighbors);
    assert!(result.is_ok());
    let result = result.unwrap();

    // Scan and dumb densities 0.0 to 1.0 at 0.01 intervals
    println!("Location\tDensity");
    for i in 0..100 {
        let location = Location::new(i as f64 / 100.0);
        let density = result.get_density_at(location).unwrap();
        // Print and round density to 2 decimals
        println!("{}\t{}", location.as_f64(), (density * 100.0).round() / 100.0);
    }

    assert_eq!(result.get_density_at(Location::new(0.2)).unwrap(), 3.0);
    assert_eq!(result.get_density_at(Location::new(0.6)).unwrap(), 2.0);
    assert_eq!(result.get_density_at(Location::new(0.4)).unwrap(), 2.5);
    assert_eq!(result.get_density_at(Location::new(0.5)).unwrap(), 2.25);
}

#[test]
fn test_drop() {
    let mut sw = RequestDensityTracker::new(4);
    sw.sample(Location::new(0.21));
    sw.sample(Location::new(0.22));
    sw.sample(Location::new(0.23));
    sw.sample(Location::new(0.61));
    sw.sample(Location::new(0.62));

    let mut neighbors = BTreeMap::new();
    neighbors.insert(Location::new(0.2), 1);
    neighbors.insert(Location::new(0.6), 1);

    let result = sw.create_density_map(&neighbors);
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.neighbor_request_counts.get(&Location::new(0.2)), Some(&2));
    assert_eq!(result.neighbor_request_counts.get(&Location::new(0.6)), Some(&2));

}

#[test]
fn test_empty_neighbors_error() {
    let sw = RequestDensityTracker::new(10);
    let empty_neighbors = BTreeMap::new();
    let result = sw.create_density_map(&empty_neighbors);
    assert!(matches!(result, Err(DensityMapError::EmptyNeighbors)));
}

#[test]
fn test_get_max_density() {
    let mut density_map = DensityMap {
        neighbor_request_counts: BTreeMap::new(),
    };

    density_map.neighbor_request_counts.insert(Location::new(0.2), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.6), 2);
    density_map.neighbor_request_counts.insert(Location::new(0.8), 2);

    let result = density_map.get_max_density();
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result, Location::new(0.7));
}

#[test]
fn test_get_max_density_2() {
    let mut density_map = DensityMap {
        neighbor_request_counts: BTreeMap::new(),
    };

    density_map.neighbor_request_counts.insert(Location::new(0.2), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.6), 2);
    density_map.neighbor_request_counts.insert(Location::new(0.8), 2);
    density_map.neighbor_request_counts.insert(Location::new(0.9), 1);

    let result = density_map.get_max_density();
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result, Location::new(0.7));
}

#[test]
fn test_get_max_density_first_last() {
    let mut density_map = DensityMap {
        neighbor_request_counts: BTreeMap::new(),
    };

    density_map.neighbor_request_counts.insert(Location::new(0.0), 2);
    density_map.neighbor_request_counts.insert(Location::new(0.2), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.6), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.8), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.9), 2);

    let result = density_map.get_max_density();
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result, Location::new(0.95));
}

#[test]
fn test_get_max_density_first_last_2() {
    // Verify the other case in max_density_location calculation
    let mut density_map = DensityMap {
        neighbor_request_counts: BTreeMap::new(),
    };

    density_map.neighbor_request_counts.insert(Location::new(0.3), 2);
    density_map.neighbor_request_counts.insert(Location::new(0.4), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.6), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.8), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.9), 2);

    let result = density_map.get_max_density();
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result, Location::new(0.1));
}

#[test]
fn test_get_max_density_first_last_3() {
    // Verify the other case in max_density_location calculation
    let mut density_map = DensityMap {
        neighbor_request_counts: BTreeMap::new(),
    };

    density_map.neighbor_request_counts.insert(Location::new(0.1), 2);
    density_map.neighbor_request_counts.insert(Location::new(0.2), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.3), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.4), 1);
    density_map.neighbor_request_counts.insert(Location::new(0.7), 2);

    let result = density_map.get_max_density();
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result, Location::new(0.9));
}

#[test]
fn test_get_max_density_empty_neighbors_error() {
    let density_map = DensityMap {
        neighbor_request_counts: BTreeMap::new(),
    };

    let result = density_map.get_max_density();
    assert!(matches!(result, Err(DensityMapError::EmptyNeighbors)));
}
