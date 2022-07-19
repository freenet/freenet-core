use std::borrow::Cow;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// A standard web `model` plane type contract base state.
///
/// This type encapsulates the web model data.
#[non_exhaustive]
pub struct WebModelState<'a> {
    pub metadata: Cow<'a, [u8]>,
    pub model_data: Cow<'a, [u8]>,
}

impl<'a> WebModelState<'a> {
    pub fn from_data(metadata: Vec<u8>, model_data: Vec<u8>) -> Self {
        Self {
            metadata: Cow::from(metadata),
            model_data: Cow::from(model_data),
        }
    }

    pub fn pack(self) -> std::io::Result<Vec<u8>> {
        let mut output = Vec::with_capacity(
            self.metadata.len() + self.model_data.len() + (std::mem::size_of::<u64>() * 2),
        );
        output.write_u64::<BigEndian>(self.metadata.len() as u64)?;
        output.extend(self.metadata.iter());
        output.write_u64::<BigEndian>(self.model_data.len() as u64)?;
        output.extend(self.model_data.iter());
        Ok(output)
    }
}

impl<'a> TryFrom<&'a [u8]> for WebModelState<'a> {
    type Error = std::io::Error;

    fn try_from(mut state: &'a [u8]) -> Result<Self, Self::Error> {
        let metadata_size = state.read_u64::<BigEndian>()?;
        let (metadata, mut rest) = state.split_at(metadata_size as usize);
        let data_size = rest.read_u64::<BigEndian>()?;
        let (model_data, rest) = rest.split_at(data_size as usize);
        if !rest.is_empty() {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        Ok(WebModelState {
            metadata: Cow::from(metadata),
            model_data: Cow::from(model_data),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn package_model() -> Result<(), Box<dyn std::error::Error>> {
        let metadata = vec![0, 2];
        let model_data = vec![5, 2, 9, 6];
        let packed = WebModelState::from_data(metadata.clone(), model_data.clone()).pack()?;
        let unpacked = WebModelState::try_from(packed.as_ref())?;
        assert_eq!(unpacked.metadata.as_ref(), &*metadata);
        assert_eq!(unpacked.model_data.as_ref(), &*model_data);
        Ok(())
    }
}
