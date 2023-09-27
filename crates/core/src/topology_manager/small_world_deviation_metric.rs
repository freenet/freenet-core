extern crate nalgebra as na;

/*
This module provides a metric called `SmallWorldDeviationMetric` to quantify
how well a given peer's connections in a peer-to-peer network approximate an
ideal small-world topology. The ideal topology is based on a 1D ring model
where the probability density of a connection at distance `r` is proportional
to `r^-1`.

The metric is calculated as follows:
1. A normalization constant `C` is calculated to ensure that the total
   probability of the ideal distribution is 1.
2. For a range of distances `X`, the ideal and actual proportions of peers
   within `X` are calculated.
3. The area between the ideal and actual curves is calculated using trapezoidal
   integration, yielding the `SmallWorldDeviationMetric`.

A metric value close to 0 indicates that the actual distribution closely matches
the ideal. A positive value indicates a lack of long-range links, and a negative
value indicates a lack of short-range links.
*/

/// Calculate the normalization constant C for the ideal r^-1 distribution.
/// The integral is approximated using trapezoidal rule.
fn calculate_normalization_constant() -> f64 {
    let mut sum = 0.0;
    let step = 0.01;
    let mut r = step;
    while r <= 0.5 {
        sum += 1.0 / r;
        r += step;
    }
    sum *= step; // Multiply by step size to complete the trapezoidal rule
    1.0 / sum // Normalize so that total probability is 1
}

/// Calculate the ideal proportion of peers within distance X for the r^-1 distribution.
fn ideal_proportion_within_x(x: f64, c: f64) -> f64 {
    c * (x.ln() - 0.01f64.ln())
}

/// Calculate the actual proportion of peers within distance X.
fn actual_proportion_within_x(connection_distances: &[f64], x: f64) -> f64 {
    let count = connection_distances.iter().filter(|&&r| r <= x).count();
    count as f64 / connection_distances.len() as f64
}

/// Calculate the SmallWorldDeviationMetric as the area between the ideal and actual curves.
/// Uses trapezoidal rule for integration.
///
/// Returns a metric where:
/// - 0.0 is ideal, indicating a perfect match with the ideal small-world topology.
/// - A negative value indicates the network is not clustered enough (lacks short-range links).
/// - A positive value indicates the network is too clustered (lacks long-range links).
pub(crate) fn small_world_deviation_metric(connection_distances: &[f64]) -> f64 {
    let c = calculate_normalization_constant();
    let mut sum = 0.0;
    let step = 0.01;
    let mut x = step;
    while x <= 0.5 {
        let ideal = ideal_proportion_within_x(x, c);
        let actual = actual_proportion_within_x(&connection_distances, x);
        sum += actual - ideal;
        x += step;
    }
    sum * step // Multiply by step size to complete the trapezoidal rule
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_world_deviation_metric() {
        // Ideal case: distances drawn from an r^-1 distribution
        let ideal_distances: Vec<f64> = vec![0.1, 0.2, 0.05, 0.4, 0.3]; // Replace with actual ideal distances
        let metric_ideal = small_world_deviation_metric(ideal_distances);
        assert!(metric_ideal.abs() < 0.1); // The metric should be close to zero for the ideal case

        // Non-ideal case 1: mostly short distances
        let non_ideal_1: Vec<f64> = vec![0.01, 0.02, 0.03, 0.04, 0.05];
        let metric_non_ideal_1 = small_world_deviation_metric(non_ideal_1);
        assert!(metric_non_ideal_1 > 0.1); // The metric should be significantly positive

        // Non-ideal case 2: mostly long distances
        let non_ideal_2: Vec<f64> = vec![0.4, 0.45, 0.48, 0.49, 0.5];
        let metric_non_ideal_2 = small_world_deviation_metric(non_ideal_2);
        assert!(metric_non_ideal_2 < -0.1); // The metric should be significantly negative
    }
}
