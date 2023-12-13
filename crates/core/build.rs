use std::process::Command;

fn main() {
    let status = Command::new("flatc")
        .arg("--rust")
        .arg("-o")
        .arg("src/generated")
        .arg("../../schemas/flatbuffers/topology.fbs")
        .status();
    if let Err(err) = status {
        println!("failed compiling flatbuffers schema: {err}");
        println!("refer to https://github.com/google/flatbuffers to install the flatc compiler");
    } else {
        let _ = Command::new("cargo").arg("fmt").status();
    }
}
