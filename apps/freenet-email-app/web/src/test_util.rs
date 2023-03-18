use std::str::FromStr;

use locutus_aft_interface::{Tier, TokenAssignment};
use locutus_stdlib::prelude::ContractInstanceId;
use rsa::{pkcs1::DecodeRsaPrivateKey, RsaPrivateKey};

pub(crate) fn test_assignment() -> TokenAssignment {
    const RSA_PRIV_PEM: &str = include_str!("../examples/rsa4096-user-priv.pem");
    let key = RsaPrivateKey::from_pkcs1_pem(RSA_PRIV_PEM).unwrap();
    TokenAssignment {
        tier: Tier::Day1,
        time_slot: Default::default(),
        assignee: key.to_public_key(),
        signature: rsa::pkcs1v15::Signature::from(vec![1; 64].into_boxed_slice()),
        assignment_hash: [0; 32],
        token_record: ContractInstanceId::from_str("7MxRGrYiBBK2rHCVpP25SxqBLco2h4zpb2szsTS7XXgg")
            .unwrap(),
    }
}
