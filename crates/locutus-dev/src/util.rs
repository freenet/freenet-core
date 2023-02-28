use std::{
    io::{self, Read, Write},
    process::Child,
    time::Duration,
};

use serde::de::DeserializeOwned;

use crate::{local_node::DeserializationFmt, DynError};

pub fn deserialize<T, R>(deser_format: Option<DeserializationFmt>, data: &R) -> Result<T, DynError>
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

pub(crate) fn pipe_std_streams(mut child: Child) -> Result<(), DynError> {
    let mut c_stdout = child.stdout.take().expect("Failed to open command stdout");
    let mut stdout = io::stdout();
    let mut stdout_buf = vec![];

    let mut c_stderr = child.stderr.take().expect("Failed to open command stderr");
    let mut stderr = io::stderr();
    let mut stderr_buf = vec![];

    let mut write_child_output = || -> Result<(), DynError> {
        c_stdout.read_to_end(&mut stdout_buf)?;
        stdout.write_all(&stdout_buf)?;
        stdout_buf.clear();

        c_stderr.read_to_end(&mut stderr_buf)?;
        stderr.write_all(&stderr_buf)?;
        stderr_buf.clear();
        Ok(())
    };

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                write_child_output()?;
                if !status.success() {
                    return Err(format!("exit with status: {status}").into());
                }
                break;
            }
            Ok(None) => {
                write_child_output()?;
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(err) => {
                write_child_output()?;
                return Err(err.into());
            }
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
