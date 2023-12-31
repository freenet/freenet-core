use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::time::Duration;

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct Rate {
    value: f64,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct RateProportion {
    value: f64,
}

impl Rate {
    pub fn new(value: f64, divisor: Duration) -> Self {
        debug_assert!(value >= 0.0, "Value must be non-negative");
        let divisor_as_secs = divisor.as_secs_f64();
        debug_assert!(divisor_as_secs > 0.0, "Divisor must be greater than 0");
        Rate {
            value: value / divisor_as_secs,
        }
    }

    pub const fn new_per_second(value: f64) -> Self {
        Rate { value }
    }

    pub const fn per_second(&self) -> f64 {
        self.value
    }

    pub fn proportion_of(&self, other: &Rate) -> RateProportion {
        debug_assert!(
            other.value > 0.0,
            "Cannot calculate proportion of zero rate"
        );
        RateProportion {
            value: self.value / other.value,
        }
    }
}

impl Eq for Rate {}

impl Ord for Rate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for Rate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd for RateProportion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl std::ops::Add for Rate {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Rate {
            value: self.value + other.value,
        }
    }
}

impl std::ops::Sub for Rate {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Rate {
            value: self.value - other.value,
        }
    }
}

impl std::ops::AddAssign for Rate {
    fn add_assign(&mut self, other: Self) {
        self.value += other.value;
    }
}

impl RateProportion {
    pub(crate) fn new(value: f64) -> Self {
        debug_assert!(
            (0.0..=1.0).contains(&value),
            "Proportion must be between 0 and 1"
        );
        RateProportion { value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate() {
        let rate = Rate::new(100.0, Duration::from_secs(2));
        assert_eq!(rate.per_second(), 50.0);
    }

    // Test that the rate is calculated correctly when the divisor is 1 second
    #[test]
    fn test_rate_per_second() {
        let rate = Rate::new_per_second(100.0);
        assert_eq!(rate.per_second(), 100.0);
    }

    // Verify that adding two rates works
    #[test]
    fn test_add() {
        let rate1 = Rate::new(100.0, Duration::from_secs(2));
        let rate2 = Rate::new(200.0, Duration::from_secs(2));
        let rate3 = rate1 + rate2;
        assert_eq!(rate3.per_second(), 150.0);
    }

    // Test that += works
    #[test]
    fn test_add_assign() {
        let mut rate1 = Rate::new(100.0, Duration::from_secs(2));
        let rate2 = Rate::new(200.0, Duration::from_secs(2));
        rate1 += rate2;
        assert_eq!(rate1.per_second(), 150.0);
    }

    // Test that subtraction works
    #[test]
    fn test_sub() {
        let rate1 = Rate::new(100.0, Duration::from_secs(2));
        let rate2 = Rate::new(200.0, Duration::from_secs(2));
        let rate3 = rate2 - rate1;
        assert_eq!(rate3.per_second(), 50.0);
    }
}
