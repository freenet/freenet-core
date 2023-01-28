use super::*;

mod token_assignment {
    use super::*;
    use chrono::NaiveDate;
    use ed25519_dalek::Signature;
    use locutus_aft_interface::Tier;

    fn get_assignment_date(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        let naive = NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        DateTime::<Utc>::from_utc(naive, Utc)
    }

    const TEST_TIER: Tier = Tier::Day1;
    const MAX_DURATION_1Y: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 3600);

    #[test]
    fn free_spot_first() {
        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![TokenAssignment {
                tier: TEST_TIER,
                time_slot: get_assignment_date(2023, 1, 25),
                assignee: vec![],
                signature: Signature::from([1; 64]),
            }],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y).unwrap(),
            get_assignment_date(2023, 1, 27),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 27));
    }

    #[test]
    fn free_spot_skip_first() {
        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 27),
                    assignee: vec![],
                    signature: Signature::from([1; 64]),
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2023, 1, 26),
                    assignee: vec![],
                    signature: Signature::from([1; 64]),
                },
            ],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y).unwrap(),
            get_assignment_date(2023, 1, 27).with_minute(1).unwrap(),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 28));
    }

    #[test]
    fn free_spot_skip_gap_1() {
        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 27),
                    assignee: vec![],
                    signature: Signature::from([1; 64]),
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 29),
                    assignee: vec![],
                    signature: Signature::from([1; 64]),
                },
            ],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y).unwrap(),
            get_assignment_date(2023, 1, 27),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 28));

        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 27),
                    assignee: vec![],
                    signature: Signature::from([1; 64]),
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 28),
                    assignee: vec![],
                    signature: Signature::from([1; 64]),
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 30),
                    assignee: vec![],
                    signature: Signature::from([1; 64]),
                },
            ],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y).unwrap(),
            get_assignment_date(2023, 1, 27).with_minute(1).unwrap(),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 29));
    }

    #[test]
    fn free_spot_new() {
        let records = TokenAllocationRecord::new(HashMap::new());
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y).unwrap(),
            get_assignment_date(2023, 1, 27).with_minute(1).unwrap(),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 28));
    }
}
