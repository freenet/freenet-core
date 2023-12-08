use std::cmp::Ordering;
use std::time::Duration;

#[derive(Debug, PartialEq)]
pub struct Rate {
    value: f64,
}

impl Rate {
    pub fn new(value: f64, divisor: Duration) -> Self {
        // Panic if divisor is zero
        if divisor.as_secs() == 0 && divisor.subsec_nanos() == 0 {
            panic!("Rate Divisor cannot be zero");
        }
        Rate {
            value: value / divisor.as_secs_f64(),
        }
    }

    pub fn per_second(&self) -> f64 {
        self.value
    }
}

impl Eq for Rate {}

impl PartialOrd for Rate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl Ord for Rate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
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
}
