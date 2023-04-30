use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use locutus_aft_interface::{Tier, TokenAllocationRecord, TokenAssignment, TokenParameters};
use locutus_stdlib::prelude::blake2::Digest;
use locutus_stdlib::prelude::ContractKey;
use pico_args::Arguments;
use rsa::pkcs1v15::Signature;
use rsa::{sha2::Sha256, Pkcs1v15Sign, RsaPrivateKey};

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");
const STATE_UPDATE: &[u8; 8] = &[168, 7, 13, 64, 168, 123, 142, 215];
static TOKEN_RECORD_CODE_HASH: &str = include_str!("../../token_allocation_record_code_hash");

struct Args {
    _key_id: String,
    generator_key_file_path: String,
    receiver_key_file_path: String,
}

fn parse_args() -> Result<Args, pico_args::Error> {
    const HELP: &str = r#"
USAGE:
    gen_initial_state [KEY-ID] [PATH-TO-PRIVATE-GENERATOR-KEY-FILE] [PATH-TO-PRIVATE-RECEIVER-KEY-FILE] 

FLAGS:
    -h, --help            Prints help information

ARGS:
    <KEY-ID> 
    <PATH-TO-PRIVATE-GENERATOR-KEY-FILE> 
    <PATH-TO-PRIVATE-RECEIVER-KEY-FILE> 
"#;
    let mut args = Arguments::from_env();
    if args.contains(["-h", "--help"]) {
        print!("{}", HELP);
        std::process::exit(0);
    }

    Ok(Args {
        _key_id: args.free_from_str()?,
        generator_key_file_path: args.free_from_str()?,
        receiver_key_file_path: args.free_from_str()?,
    })
}

fn load_key(key_file_path: impl AsRef<Path>) -> Result<RsaPrivateKey, Box<dyn std::error::Error>> {
    let mut key_file = File::open(key_file_path)?;
    let mut buffer = Vec::new();
    key_file.read_to_end(&mut buffer)?;
    let private_key_str = std::str::from_utf8(&buffer)?;
    let private_key =
        <RsaPrivateKey as rsa::pkcs1::DecodeRsaPrivateKey>::from_pkcs1_pem(private_key_str)
            .map_err(|e| format!("{e}"))?;
    Ok(private_key)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Args {
        generator_key_file_path,
        receiver_key_file_path,
        ..
    } = parse_args()?;

    let generator_private_key = load_key(&generator_key_file_path)?;
    let receiver_private_key = load_key(&receiver_key_file_path)?;

    let inbox_path = PathBuf::from(MANIFEST);
    let digest = Sha256::digest(STATE_UPDATE).to_vec();
    let signature = generator_private_key
        .sign(Pkcs1v15Sign::new::<Sha256>(), &digest)
        .map_err(|e| format!("{e}"))?;

    let naive = NaiveDate::from_ymd_opt(2023, 1, 25)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let slot = DateTime::<Utc>::from_utc(naive, Utc);
    let record_params = TokenParameters::new(generator_private_key.to_public_key());
    let token_record =
        ContractKey::from_params(TOKEN_RECORD_CODE_HASH, record_params.try_into()?)?.into();
    let tokens = {
        // FIXME: in the future don't hardcode any TokenAssignment
        let token_assignment = TokenAssignment {
            tier: Tier::Day1,
            time_slot: slot,
            assignee: receiver_private_key.to_public_key(),
            signature: Signature::from(signature.into_boxed_slice()),
            assignment_hash: [0; 32],
            token_record,
        };

        let mut tokens: HashMap<Tier, Vec<TokenAssignment>> = HashMap::new();
        tokens.insert(Tier::Day1, vec![token_assignment]);
        tokens
    };
    let token_allocation_record: TokenAllocationRecord = TokenAllocationRecord::new(tokens);
    let state: Vec<u8> = bincode::serialize(&token_allocation_record)?;
    std::fs::write(inbox_path.join("examples").join("initial_state"), state)?;
    Ok(())
}
