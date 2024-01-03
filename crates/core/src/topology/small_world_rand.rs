#[cfg(test)]
pub(super) mod test_utils {
    use rand::Rng;

    use crate::ring::Distance;

    // Function to generate a random link distance based on Kleinberg's d^{-1} distribution
    pub(in crate::topology) fn random_link_distance(d_min: Distance) -> Distance {
        let d_max = 0.5;

        // Generate a uniform random number between 0 and 1
        let u: f64 = rand::thread_rng().gen_range(0.0..1.0);

        // Correct Inverse CDF: F^{-1}(u) = d_min * (d_max / d_min).powf(u)
        let d = d_min.as_f64() * (d_max / d_min.as_f64()).powf(u);

        Distance::new(d)
    }
}

#[cfg(test)]
mod tests {
    use crate::ring::Distance;

    use super::test_utils::*;
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
        let expected_counts: Vec<_> = (0..num_bins)
            .map(|i| {
                let lower = d_min + (d_max - d_min) * (i as f64 / num_bins as f64);
                let upper = d_min + (d_max - d_min) * ((i as f64 + 1.0) / num_bins as f64);
                ((upper - lower) / (upper.powf(-1.0) - lower.powf(-1.0))).floor() * n as f64
                    / num_bins as f64
            })
            .collect();

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
}
