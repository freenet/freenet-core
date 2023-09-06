use std::process::Command;

fn main() {
    let status = Command::new("flatc")
        .arg("--rust")
        .arg("-o")
        .arg("src")
        .arg("../../schemas/flatbuffers/common.fbs")
        .arg("../../schemas/flatbuffers/client_request.fbs")
        .arg("../../schemas/flatbuffers/host_response.fbs")
        .status();
    if let Err(err) = status {
        println!("failed compiling flatbuffers schema: {err}")
    }
    // TODO try to install flatc from https://github.com/google/flatbuffers and use it to compile schemas
}
