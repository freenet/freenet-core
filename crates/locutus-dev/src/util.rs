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

pub fn set_cleanup_on_exit() {
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
    });
}
