use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize)]
pub(super) struct Mean {
    sum: f64,
    count: u64,
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

    pub fn compute(&self) -> f64 {
        self.sum / self.count as f64
    }
}

impl Default for Mean {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(super) struct TransferSpeed {
    pub bytes_per_second: f64,
}
