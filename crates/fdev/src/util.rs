use std::{
    io::{self, Read, Write},
    process::Child,
    time::Duration,
};

use serde::de::DeserializeOwned;

use crate::wasm_runtime::DeserializationFmt;

pub fn deserialize<T, R>(
    deser_format: Option<DeserializationFmt>,
    data: &R,
) -> Result<T, anyhow::Error>
where
    T: DeserializeOwned,
    R: AsRef<[u8]> + ?Sized,
{
    match deser_format {
        Some(DeserializationFmt::Json) => {
            let deser = serde_json::from_slice(data.as_ref())?;
            Ok(deser)
        }
        #[cfg(feature = "messagepack")]
        Some(DeserializationFmt::MessagePack) => {
            let deser = rmp_serde::decode::from_read(data.as_ref())?;
            Ok(deser)
        }
        _ => Ok(bincode::deserialize(data.as_ref())?),
    }
}

pub(crate) fn pipe_std_streams(mut child: Child) -> Result<(), anyhow::Error> {
    let c_stdout = child.stdout.take().expect("Failed to open command stdout");
    let c_stderr = child.stderr.take().expect("Failed to open command stderr");

    let write_child_stderr = move || -> Result<(), anyhow::Error> {
        let mut stderr = io::stderr();
        for b in c_stderr.bytes() {
            let b = b?;
            stderr.write_all(&[b])?;
        }
        Ok(())
    };

    let write_child_stdout = move || -> Result<(), anyhow::Error> {
        let mut stdout = io::stdout();
        for b in c_stdout.bytes() {
            let b = b?;
            stdout.write_all(&[b])?;
        }
        Ok(())
    };
    std::thread::spawn(write_child_stdout);
    std::thread::spawn(write_child_stderr);

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    anyhow::bail!("exit with status: {status}");
                }
                break;
            }
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    Ok(())
}
