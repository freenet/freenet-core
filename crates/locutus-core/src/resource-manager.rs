struct ResourceManager {
    meter : Meter,
}

impl ResourceManager {
    pub fn new() -> Self {
        ResourceManager {
            meter : Meter::new(),
        }
    }

    pub fn get_meter(&self) -> &Meter {
        &self.meter
    }
}

pub struct Limits {
    pub max_upstream_bandwidth : BytesPerSecond,
    pub max_downstream_bandwidth : BytesPerSecond,
    pub max_cpu_usage : InstructionsPerSecond,
    pub max_memory_usage : f64,
    pub max_storage_usage : f64,
}

pub struct BytesPerSecond(f64);
impl BytesPerSecond {
    pub fn new(bytes_per_second : f64) -> Self {
        BytesPerSecond(bytes_per_second)
    }
}

impl Into<f64> for BytesPerSecond {
    fn into(self) -> f64 {
        self.0
    }
}

pub struct InstructionsPerSecond(f64);

impl InstructionsPerSecond {
    pub fn new(instructions_per_second : f64) -> Self {
        InstructionsPerSecond(instructions_per_second)
    }
}

impl Into<f64> for InstructionsPerSecond {
    fn into(self) -> f64 {
        self.0
    }
}

pub struct Bytes(u64);

pub impl Bytes {
    pub fn new(bytes : u64) -> Self {
        Bytes(bytes)
    }
}

impl Into<f64> for Bytes {
    fn into(self) -> f64 {
        self.0 as f64
    }
}