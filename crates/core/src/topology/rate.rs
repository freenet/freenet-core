mod impls;
mod tests;

use serde::{Deserialize, Serialize};
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

impl RateProportion {
    pub(crate) fn new(value: f64) -> Self {
        debug_assert!(
            (0.0..=1.0).contains(&value),
            "Proportion must be between 0 and 1"
        );
        RateProportion { value }
    }
}
