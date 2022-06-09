use std::{fs::File, io::Write, path::PathBuf};

use byteorder::{BigEndian, WriteBytesExt};

const CONTRACT_DIR: &str = env!("CARGO_MANIFEST_DIR");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_state()
}

fn build_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut complete_state = Vec::new();
    append_metadata(&mut complete_state)?;
    append_web_content(&mut complete_state)?;
    append_dynamic_state(&mut complete_state)?;
    let mut state = File::create(PathBuf::from(CONTRACT_DIR).join("encoded_state"))?;
    state.write_all(&complete_state)?;
    Ok(())
}

fn append_metadata(state: &mut Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    let metadata: &[u8] = &[];
    state.write_u64::<BigEndian>(metadata.len() as u64)?;
    Ok(())
}

fn append_web_content(state: &mut Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    let web_tar = {
        let web_content = Vec::new();
        let content_path = PathBuf::from("web");
        let mut tar = tar::Builder::new(web_content);
        tar.append_path(content_path.join("index.html"))?;
        tar.append_path(content_path.join("state.html"))?;
        tar.into_inner()?
    };
    let mut encoded = vec![];
    {
        let mut encoder = xz2::write::XzEncoder::new(&mut encoded, 6);
        encoder.write_all(&web_tar)?;
    }
    state.write_u64::<BigEndian>(encoded.len() as u64)?;
    state.append(&mut encoded);
    Ok(())
}

fn append_dynamic_state(state: &mut Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    let dynamic_state = PathBuf::from(CONTRACT_DIR).join("initial_state.json");
    let json: serde_json::Value = serde_json::from_reader(File::open(dynamic_state)?)?;
    let mut bytes = serde_json::to_vec(&json)?;
    state.write_u64::<BigEndian>(bytes.len() as u64)?;
    state.append(&mut bytes);
    Ok(())
}
