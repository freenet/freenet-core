use std::time::Duration;

#[derive(Debug, PartialEq)]
pub struct Rate {
    value: f64,
}

impl Rate {
    pub fn new(value: f64, divisor: Duration) -> Self {
        Rate {
            value: value / divisor.as_secs_f64(),
        }
    }

    pub fn per_second(&self) -> f64 {
        self.value
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
