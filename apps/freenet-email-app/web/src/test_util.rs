use std::str::FromStr;

use freenet_aft_interface::{Tier, TokenAssignment};
use freenet_stdlib::prelude::ContractInstanceId;
use rand_chacha::rand_core::OsRng;
use rsa::RsaPrivateKey;

pub(crate) fn test_assignment() -> TokenAssignment {
    let key = RsaPrivateKey::new(&mut OsRng, 32).unwrap();
    TokenAssignment {
        tier: Tier::Day1,
        time_slot: Default::default(),
        generator: key.to_public_key(),
        signature: rsa::pkcs1v15::Signature::try_from([1u8; 64].as_slice()).unwrap(),
        assignment_hash: [0; 32],
        token_record: ContractInstanceId::from_str("7MxRGrYiBBK2rHCVpP25SxqBLco2h4zpb2szsTS7XXgg")
            .unwrap(),
    }
}
