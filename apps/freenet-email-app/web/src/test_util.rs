use std::str::FromStr;

use locutus_aft_interface::{Tier, TokenAssignment};
use locutus_stdlib::prelude::ContractInstanceId;
use rsa::{pkcs1::DecodeRsaPrivateKey, RsaPrivateKey};

pub(crate) fn test_assignment() -> TokenAssignment {
    const RSA_PRIV_PEM: &str = include_str!("../examples/rsa4096-id-1-priv.pem");
    let key = RsaPrivateKey::from_pkcs1_pem(RSA_PRIV_PEM).unwrap();
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
