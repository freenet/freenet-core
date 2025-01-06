use super::*;

#[test]
fn test_rate() {
    let rate = Rate::new(100.0, Duration::from_secs(2));
    assert_eq!(rate.per_second(), 50.0);
}

#[test]
fn test_rate_per_second() {
    let rate = Rate::new_per_second(100.0);
    assert_eq!(rate.per_second(), 100.0);
}

#[test]
fn test_add() {
    let rate1 = Rate::new(100.0, Duration::from_secs(2));
    let rate2 = Rate::new(200.0, Duration::from_secs(2));
    let rate3 = rate1 + rate2;
    assert_eq!(rate3.per_second(), 150.0);
}

#[test]
fn test_add_assign() {
    let mut rate1 = Rate::new(100.0, Duration::from_secs(2));
    let rate2 = Rate::new(200.0, Duration::from_secs(2));
    rate1 += rate2;
    assert_eq!(rate1.per_second(), 150.0);
}

#[test]
fn test_sub() {
    let rate1 = Rate::new(100.0, Duration::from_secs(2));
    let rate2 = Rate::new(200.0, Duration::from_secs(2));
    let rate3 = rate2 - rate1;
    assert_eq!(rate3.per_second(), 50.0);
}
