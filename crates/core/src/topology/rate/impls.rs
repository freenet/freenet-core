use super::{Rate, RateProportion};
use std::cmp::Ordering;

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
