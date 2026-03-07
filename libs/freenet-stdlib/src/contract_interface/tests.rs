use std::sync::LazyLock;

use super::*;
use crate::parameters::Parameters;
use rand::{rng, rngs::SmallRng, Rng, SeedableRng};

static RND_BYTES: LazyLock<[u8; 1024]> = LazyLock::new(|| {
    let mut bytes = [0; 1024];
    let mut rng = SmallRng::from_rng(&mut rng());
    rng.fill(&mut bytes);
    bytes
});

#[test]
fn key_encoding() -> Result<(), Box<dyn std::error::Error>> {
    let code = ContractCode::from(vec![1, 2, 3]);
    let expected = ContractKey::from_params_and_code(Parameters::from(vec![]), &code);
    // let encoded_key = expected.encode();
    // println!("encoded key: {encoded_key}");
    // let encoded_code = expected.contract_part_as_str();
    // println!("encoded key: {encoded_code}");

    let decoded = ContractKey::from_params(code.hash_str(), [].as_ref().into())?;
    assert_eq!(expected, decoded);
    assert_eq!(expected.code_hash(), decoded.code_hash());
    Ok(())
}

#[test]
fn key_ser() -> Result<(), Box<dyn std::error::Error>> {
    let mut gen = arbitrary::Unstructured::new(&*RND_BYTES);
    let expected: ContractKey = gen.arbitrary()?;
    let encoded = bs58::encode(expected.as_bytes()).into_string();
    // println!("encoded key: {encoded}");

    let serialized = bincode::serialize(&expected)?;
    let deserialized: ContractKey = bincode::deserialize(&serialized)?;
    let decoded = bs58::encode(deserialized.as_bytes()).into_string();
    assert_eq!(encoded, decoded);
    assert_eq!(deserialized, expected);
    Ok(())
}

#[test]
fn contract_ser() -> Result<(), Box<dyn std::error::Error>> {
    let mut gen = arbitrary::Unstructured::new(&*RND_BYTES);
    let expected: Contract = gen.arbitrary()?;

    let serialized = bincode::serialize(&expected)?;
    let deserialized: Contract = bincode::deserialize(&serialized)?;
    assert_eq!(deserialized, expected);
    Ok(())
}
