use super::*;

mod token_assignment {
    use super::*;
    use chrono::{NaiveDate, Timelike};
    use freenet_aft_interface::Tier;
    use once_cell::sync::Lazy;
    use rsa::{pkcs1v15::Signature, RsaPublicKey};

    fn get_assignment_date(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        let naive = NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc)
    }

    const TEST_TIER: Tier = Tier::Day1;
    const MAX_DURATION_1Y: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 3600);

    const RSA_4096_PUB_PEM: &str = include_str!("../../../interfaces/examples/rsa4096-pub.pem");
    static PK: Lazy<RsaPublicKey> = Lazy::new(|| {
        <RsaPublicKey as rsa::pkcs1::DecodeRsaPublicKey>::from_pkcs1_pem(RSA_4096_PUB_PEM).unwrap()
    });

    static ID: Lazy<ContractInstanceId> = Lazy::new(|| {
        let rnd = [1; 32];
        let mut gen = arbitrary::Unstructured::new(&rnd);
        gen.arbitrary().unwrap()
    });

    #[test]
    fn free_spot_first() {
        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![TokenAssignment {
                tier: TEST_TIER,
                time_slot: get_assignment_date(2023, 1, 25),
                generator: PK.clone(),
                signature: Signature::try_from([1u8; 64].as_slice()).unwrap(),
                assignment_hash: [0; 32],
                token_record: *ID,
            }],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
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
                    generator: PK.clone(),
                    signature: Signature::try_from([1u8; 64].as_slice()).unwrap(),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2023, 1, 26),
                    generator: PK.clone(),
                    signature: Signature::try_from([1u8; 64].as_slice()).unwrap(),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
            ],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
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
                    generator: PK.clone(),
                    signature: Signature::try_from([1u8; 64].as_slice()).unwrap(),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 29),
                    generator: PK.clone(),
                    signature: Signature::try_from([1u8; 64].as_slice()).unwrap(),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
            ],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
            get_assignment_date(2023, 1, 27),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 28));

        let records = TokenAllocationRecord::new(HashMap::from_iter([(
            TEST_TIER,
            vec![
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 27),
                    generator: PK.clone(),
                    signature: Signature::try_from([1u8; 64].as_slice()).unwrap(),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 28),
                    generator: PK.clone(),
                    signature: Signature::try_from([1u8; 64].as_slice()).unwrap(),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
                TokenAssignment {
                    tier: TEST_TIER,
                    time_slot: get_assignment_date(2022, 1, 30),
                    generator: PK.clone(),
                    signature: Signature::try_from([1u8; 64].as_slice()).unwrap(),
                    assignment_hash: [0; 32],
                    token_record: *ID,
                },
            ],
        )]));
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
            get_assignment_date(2023, 1, 27).with_minute(1).unwrap(),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 29));
    }

    #[test]
    fn free_spot_new() {
        let records = TokenAllocationRecord::new(HashMap::new());
        let assignment = records.next_free_assignment(
            &AllocationCriteria::new(TEST_TIER, MAX_DURATION_1Y, *ID).unwrap(),
            get_assignment_date(2023, 1, 27).with_minute(1).unwrap(),
        );
        assert_eq!(assignment.unwrap(), get_assignment_date(2022, 1, 28));
    }
}
