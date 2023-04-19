struct ResourceManager {
    meter : Meter,
}

impl ResourceManager {
    pub fn new() -> Self {
        ResourceManager {
            meter : Meter::new(),
        }
    }
}