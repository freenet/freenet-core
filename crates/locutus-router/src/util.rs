use std::time::Duration;

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Mean {
    pub sum: f64,
    pub count: u64,
}

impl Mean {
    pub fn new() -> Self {
        Mean { sum: 0.0, count: 0 }
    }

    pub fn add(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }

    pub fn add_with_count(&mut self, sum: f64, count: u64) {
        self.sum += sum;
        self.count += count;
    }

    pub fn get(&self) -> f64 {
        self.sum / self.count as f64
    }
}

impl Default for Mean {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct TransferSpeed {
    pub(crate) bytes_per_second: f64,
}

impl TransferSpeed {
    pub fn new(bytes: usize, duration: Duration) -> Self {
        TransferSpeed {
            bytes_per_second: bytes as f64 / duration.as_secs_f64(),
        }
    }
}
