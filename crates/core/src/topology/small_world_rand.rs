use rand::Rng;

use crate::ring::Distance;

// Function to generate a random link distance based on Kleinberg's d^{-1} distribution
pub(crate) fn random_link_distance(d_min: Distance) -> Distance {
    let d_max = 0.5;

    // Generate a uniform random number between 0 and 1
    let u: f64 = rand::thread_rng().gen_range(0.0..1.0);

    // Correct Inverse CDF: F^{-1}(u) = d_min * (d_max / d_min).powf(u)
    let d = d_min.as_f64() * (d_max / d_min.as_f64()).powf(u);

    Distance::new(d)
}

#[cfg(test)]
mod tests {
    use crate::topology::metric::measure_small_worldness;

    use super::*;
    use statrs::distribution::*;

    #[test]
    fn chi_squared_test() {
        let d_min = 0.01;
        let d_max = 0.5;
        let n = 10000; // Number of samples
        let num_bins = 20; // Number of bins for histogram
        let mut bins = vec![0; num_bins];

        // Generate a bunch of link distances
        for _ in 0..n {
            let d = random_link_distance(Distance::new(d_min)).as_f64();
            let bin_index = ((d - d_min) / (d_max - d_min) * (num_bins as f64)).floor() as usize;
            if bin_index < num_bins {
                bins[bin_index] += 1;
            }
        }

        // Perform chi-squared test
        let mut expected_counts = vec![0.0; num_bins];
        for i in 0..num_bins {
            let lower = d_min + (d_max - d_min) * (i as f64 / num_bins as f64);
            let upper = d_min + (d_max - d_min) * ((i as f64 + 1.0) / num_bins as f64);
            expected_counts[i] = ((upper - lower) / (upper.powf(-1.0) - lower.powf(-1.0))).floor()
                * n as f64
                / num_bins as f64;
        }

        let chi_squared = expected_counts
            .iter()
            .zip(bins.iter())
            .map(|(&e, &o)| ((o as f64 - e) * (o as f64 - e)) / e)
            .sum::<f64>();

        // Degrees of freedom is num_bins - 1
        let dof = num_bins - 1;
        let chi = ChiSquared::new(dof as f64).unwrap();

        let p_value = 1.0 - chi.cdf(chi_squared);

        // Check if p_value is above 0.05, indicating that we fail to reject the null hypothesis
        assert!(
            p_value > 0.05,
            "Chi-squared test failed, p_value = {}",
            p_value
        );
    }

    #[test]
    fn metric_test() {
        let d_min = Distance::new(0.01);
        let n = 1000; // Number of samples
        let mut distances = vec![];
        // Generate a bunch of link distances
        for _ in 0..n {
            distances.push(random_link_distance(d_min));
        }

        let metric = measure_small_worldness(&distances);

        println!("Small-world deviation metric = {}", metric);

        // Check if metric is close to 0.0, indicating that the network is close to the ideal small-world topology
        assert!(
            metric.abs() < 0.1,
            "Small-world deviation metric is too high, metric = {}",
            metric
        );
    }
}
