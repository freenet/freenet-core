use locutus_node::WrappedState;
use std::io::Read;

pub fn get_random_state() -> WrappedState {
    // Prepare contract state
    let mut web = vec![];
    let mut file_obj = std::fs::File::open("src/example/web.tar.xz").unwrap();
    let web_size = (file_obj.read_to_end(&mut web).unwrap() as u64)
        .to_be_bytes()
        .to_vec();

    let metadata: Vec<u8> = "metadata".as_bytes().to_vec();
    let metadata_size: Vec<u8> = u64::try_from(metadata.len())
        .unwrap()
        .to_be_bytes()
        .to_vec();
    let reminder: Vec<u8> = "reminder".as_bytes().to_vec();
    let state_vec: Vec<u8> = [metadata_size, metadata, web_size, web, reminder].concat();
    WrappedState::new(state_vec)
}
