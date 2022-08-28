use std::{
    io::{self, Read, Write},
    process::Child,
    time::Duration,
};

use serde::de::DeserializeOwned;

use crate::{config::DeserializationFmt, DynError};

pub fn deserialize<T, R>(deser_format: Option<DeserializationFmt>, data: &R) -> Result<T, DynError>
where
    T: DeserializeOwned,
    R: AsRef<[u8]> + ?Sized,
{
    match deser_format {
        #[cfg(feature = "json")]
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

pub fn set_cleanup_on_exit() -> Result<(), ctrlc::Error> {
    ctrlc::set_handler(move || {
        tracing::info!("Received Ctrl+C. Cleaning up...");

        let path = std::env::temp_dir().join("locutus");
        tracing::info!("Removing content stored at {path:?}");

        let rm = std::process::Command::new("rm")
            .arg("-rf")
            .arg(path.to_str().expect("correct path to locutus tmp"))
            .spawn();

        if rm.is_ok() {
            tracing::info!("Successful cleanup");

            std::process::exit(0);
        } else {
            tracing::error!("Failed to remove content at {path:?}");

            std::process::exit(-1);
        }
    })
}

pub(crate) fn pipe_std_streams(mut child: Child) -> Result<(), DynError> {
    let mut c_stdout = child.stdout.take().expect("Failed to open command stdout");
    let mut stdout = io::stdout();
    let mut stdout_buf = vec![];

    let mut c_stderr = child.stderr.take().expect("Failed to open command stderr");
    let mut stderr = io::stderr();
    let mut stderr_buf = vec![];

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return Err(format!("exist with status: {status}").into());
                }
                break;
            }
            Ok(None) => {
                // attempt to write output to parent stds
                c_stdout.read_to_end(&mut stdout_buf)?;
                stdout.write_all(&stdout_buf)?;
                stdout_buf.clear();

                c_stderr.read_to_end(&mut stderr_buf)?;
                stderr.write_all(&stderr_buf)?;
                stderr_buf.clear();
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(err) => return Err(err.into()),
        }
    }

    // write out any remaining input
    c_stdout.read_to_end(&mut stdout_buf)?;
    stdout.write_all(&stdout_buf)?;
    stdout_buf.clear();

    c_stderr.read_to_end(&mut stderr_buf)?;
    stderr.write_all(&stderr_buf)?;
    stderr_buf.clear();

    Ok(())
}
