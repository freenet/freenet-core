use std::process::Command;

fn main() {
    // TODO try to install flatc from https://github.com/google/flatbuffers and use it to compile schemas
    let status = Command::new("flatc")
        .arg("--rust")
        .arg("-o")
        .arg("../../target/flatbuffers/")
        .arg("../../schemas/flatbuffers/common.fbs")
        .arg("../../schemas/flatbuffers/client_request.fbs")
        .arg("../../schemas/flatbuffers/host_response.fbs")
        .status()
        .unwrap();
    assert!(status.success());
}
