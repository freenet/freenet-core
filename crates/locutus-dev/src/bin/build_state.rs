use byteorder::{BigEndian, WriteBytesExt};
use clap::Parser;
use std::{fs::File, io::Write, path::PathBuf};

use locutus_dev::{ContractType, StateConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("error"));
    let cli = StateConfig::parse();

    let mut complete_state = Vec::new();

    let source_path: PathBuf = cli.input_path;
    let dest_file: PathBuf = cli.output_file;

    match cli.contract_type {
        ContractType::Web => {
            build_web_state(&mut complete_state, source_path, dest_file)?;
        }
        ContractType::Data => {
            build_data_state(&mut complete_state, source_path, dest_file)?;
        }
    }
    Ok(())
}

fn build_web_state(
    complete_state: &mut Vec<u8>,
    source_path: PathBuf,
    dest_file: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Bundling web state from {source_path:?} into {dest_file:?}");
    append_metadata(complete_state)?;
    append_web_content(complete_state, source_path)?;
    let mut state = File::create(dest_file)?;
    state.write_all(complete_state)?;
    Ok(())
}

fn build_data_state(
    complete_state: &mut Vec<u8>,
    source_path: PathBuf,
    dest_file: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Bundling data contract state from {source_path:?} into {dest_file:?}");
    append_metadata(complete_state)?;
    append_dynamic_state(complete_state, source_path)?;
    let mut state = File::create(dest_file)?;
    state.write_all(complete_state)?;
    Ok(())
}

fn append_metadata(state: &mut Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    let metadata: &[u8] = &[];
    state.write_u64::<BigEndian>(metadata.len() as u64)?;
    Ok(())
}

fn append_web_content(
    state: &mut Vec<u8>,
    source_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let web_tar = {
        let web_content = Vec::new();
        let mut tar = tar::Builder::new(web_content);
        tar.append_file(
            "index.html",
            &mut File::open(source_path.join("index.html"))?,
        )?;
        tar.append_file(
            "state.html",
            &mut File::open(source_path.join("state.html"))?,
        )?;
        tar.into_inner()?
    };
    assert!(!web_tar.is_empty());
    let mut encoded = vec![];
    {
        let mut encoder = xz2::write::XzEncoder::new(&mut encoded, 6);
        encoder.write_all(&web_tar)?;
        encoder.flush()?;
    }
    assert!(!encoded.is_empty());
    state.write_u64::<BigEndian>(encoded.len() as u64)?;
    state.append(&mut encoded);
    Ok(())
}

fn append_dynamic_state(
    state: &mut Vec<u8>,
    source_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let dynamic_state = source_path.join("initial_state.json");
    let json: serde_json::Value = serde_json::from_reader(File::open(dynamic_state)?)?;
    let mut bytes = serde_json::to_vec(&json)?;
    state.write_u64::<BigEndian>(bytes.len() as u64)?;
    state.append(&mut bytes);
    Ok(())
}
