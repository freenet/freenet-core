use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use chrono::{DateTime, NaiveDate, Utc};
use locutus_stdlib::prelude::blake2::Digest;
use locutus_stdlib::prelude::ContractKey;
use rsa::{sha2::Sha256, Pkcs1v15Sign, RsaPrivateKey};
use rsa::pkcs1v15::Signature;
use locutus_aft_interface::{Tier, TokenAssignment, TokenParameters};

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");
const STATE_UPDATE: &[u8; 8] = &[168, 7, 13, 64, 168, 123, 142, 215];
static TOKEN_RECORD_CODE_HASH: &str = include_str!("/home/minion/workspace/locutus/apps/freenet-email-app/web/examples/token_allocation_record_code_hash");

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <key-id> <path-to-private-generator-key-file> <path-to-private-receiver-key-file>", args[0]);
        std::process::exit(1);
    }
    println!("args: {:?}", args);
    let generator_key_file_path = &args[2];
    let receiver_key_file_path = &args[3];

    // Open the key file and read its contents into a buffer
    let mut generator_key_file = File::open(generator_key_file_path).unwrap();
    let mut generator_buffer = Vec::new();
    generator_key_file.read_to_end(&mut generator_buffer).unwrap();
    let generator_private_key_str = std::str::from_utf8(&generator_buffer).unwrap();
    let generator_private_key =
        <RsaPrivateKey as rsa::pkcs1::DecodeRsaPrivateKey>::from_pkcs1_pem(generator_private_key_str)
            .unwrap();

    let mut receiver_key_file = File::open(receiver_key_file_path).unwrap();
    let mut receiver_buffer = Vec::new();
    receiver_key_file.read_to_end(&mut receiver_buffer).unwrap();
    let receiver_private_key_str = std::str::from_utf8(&receiver_buffer).unwrap();
    let receiver_private_key =
        <RsaPrivateKey as rsa::pkcs1::DecodeRsaPrivateKey>::from_pkcs1_pem(receiver_private_key_str)
            .unwrap();

    let inbox_path = PathBuf::from(MANIFEST);
    let digest = Sha256::digest(STATE_UPDATE).to_vec();
    let signature = generator_private_key
        .sign(Pkcs1v15Sign::new::<Sha256>(), &digest)
        .unwrap();

    let naive = NaiveDate::from_ymd_opt(2023, 1, 25)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let slot = DateTime::<Utc>::from_utc(naive, Utc);
    let record_params = TokenParameters::new(generator_private_key.to_public_key());
    let token_record =
        ContractKey::from_params(TOKEN_RECORD_CODE_HASH, record_params.try_into().unwrap())
            .unwrap()
            .into();
    let token_assignment = TokenAssignment {
        tier: Tier::Day1,
        time_slot: slot,
        assignee: receiver_private_key.to_public_key(),
        signature: Signature::from(signature.into_boxed_slice()),
        assignment_hash: [0; 32],
        token_record,
    };

    let mut tokens = HashMap::new();
    tokens.insert(Tier::Day1, token_assignment);

    let state: Vec<u8> = bincode::serialize(&tokens).unwrap();

    std::fs::write(
        inbox_path.join("examples").join("initial_state.json"),
        state,
    )
    .unwrap();
}
