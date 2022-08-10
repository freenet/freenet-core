use std::borrow::Cow;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// A standard web `model` plane type contract base state.
///
/// This type encapsulates the web model data.
#[non_exhaustive]
pub struct ControllerState<'a> {
    pub metadata: Cow<'a, [u8]>,
    pub controller_data: Cow<'a, [u8]>,
}

impl<'a> ControllerState<'a> {
    pub fn from_data(metadata: Vec<u8>, controller_data: Vec<u8>) -> Self {
        Self {
            metadata: Cow::from(metadata),
            controller_data: Cow::from(controller_data),
        }
    }

    pub fn pack(self) -> std::io::Result<Vec<u8>> {
        let mut output = Vec::with_capacity(
            self.metadata.len() + self.controller_data.len() + (std::mem::size_of::<u64>() * 2),
        );
        output.write_u64::<BigEndian>(self.metadata.len() as u64)?;
        output.extend(self.metadata.iter());
        output.write_u64::<BigEndian>(self.controller_data.len() as u64)?;
        output.extend(self.controller_data.iter());
        Ok(output)
    }
}

impl<'a> TryFrom<&'a [u8]> for ControllerState<'a> {
    type Error = std::io::Error;

    fn try_from(mut state: &'a [u8]) -> Result<Self, Self::Error> {
        let metadata_size = state.read_u64::<BigEndian>()?;
        let (metadata, mut rest) = state.split_at(metadata_size as usize);
        let data_size = rest.read_u64::<BigEndian>()?;
        let (controller_data, rest) = rest.split_at(data_size as usize);
        if !rest.is_empty() {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        Ok(ControllerState {
            metadata: Cow::from(metadata),
            controller_data: Cow::from(controller_data),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn package_model() -> Result<(), Box<dyn std::error::Error>> {
        let metadata = vec![0, 2];
        let controller_data = vec![5, 2, 9, 6];
        let packed = ControllerState::from_data(metadata.clone(), controller_data.clone()).pack()?;
        let unpacked = ControllerState::try_from(packed.as_ref())?;
        assert_eq!(unpacked.metadata.as_ref(), &*metadata);
        assert_eq!(unpacked.controller_data.as_ref(), &*controller_data);
        Ok(())
    }
}
