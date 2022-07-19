use byteorder::{BigEndian, WriteBytesExt};
use clap::Parser;
use locutus_runtime::locutus_stdlib::web::model::WebModelState;
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};

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
        ContractType::Model => {
            build_model_state(source_path, dest_file)?;
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
    // FIXME: use instead WebModelState
    append_metadata(complete_state)?;
    append_web_content(complete_state, source_path)?;
    let mut state = File::create(dest_file)?;
    state.write_all(complete_state)?;
    Ok(())
}

fn build_model_state(
    source_path: PathBuf,
    dest_file: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Bundling `model` contract state from {source_path:?} into {dest_file:?}");
    // FIXME: optionally provide a path to the metadata
    // 4. from both bundle them in a `WebModelState`
    let mut model = vec![];
    let mut model_f = File::open(source_path)?;
    model_f.read_to_end(&mut model)?;
    let model = WebModelState::from_data(vec![], model);

    let mut state = File::create(dest_file)?;
    state.write_all(model.pack()?.as_slice())?;
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
