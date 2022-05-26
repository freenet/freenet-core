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
