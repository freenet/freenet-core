use std::time::Duration;

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
