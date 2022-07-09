//! Helper functions and types for dealing with HTTP gateway compatible contracts.
use std::{
    io::{Cursor, Read},
    path::Path,
};

use byteorder::{BigEndian, ReadBytesExt};
use tar::Archive;
use xz2::read::XzDecoder;

use crate::interface::State;

#[derive(Debug, thiserror::Error)]
pub enum WebContractError {
    #[error("unpacking error: {0}")]
    UnpackingError(Box<dyn std::error::Error>),
    #[error(transparent)]
    StoringError(std::io::Error),
    #[error("file not found: {0}")]
    FileNotFound(String),
}

pub struct UnpackedWeb {
    pub metadata: Vec<u8>,
    web: Archive<XzDecoder<Cursor<Vec<u8>>>>,
}

impl UnpackedWeb {
    pub fn store(&mut self, dst: impl AsRef<Path>) -> Result<(), WebContractError> {
        self.web
            .unpack(dst)
            .map_err(WebContractError::StoringError)?;
        Ok(())
    }

    pub fn get_file(&mut self, path: &str) -> Result<Vec<u8>, WebContractError> {
        for e in self
            .web
            .entries()
            .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?
        {
            let mut e = e.map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;
            if e.path()
                .ok()
                .filter(|p| p.to_string_lossy() == path)
                .is_some()
            {
                let mut bytes = vec![];
                e.read_to_end(&mut bytes)
                    .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;
                return Ok(bytes);
            }
        }
        Err(WebContractError::FileNotFound(path.to_owned()))
    }
}

impl<'a> TryFrom<State<'a>> for UnpackedWeb {
    type Error = WebContractError;

    fn try_from(mut state: State) -> Result<Self, Self::Error> {
        // Decompose the state and extract the compressed web interface
        let mut state = Cursor::new(state);

        let metadata_size = state
            .read_u64::<BigEndian>()
            .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;
        let mut metadata = vec![0; metadata_size as usize];
        state
            .read_exact(&mut metadata)
            .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;
        let web_size = state
            .read_u64::<BigEndian>()
            .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;
        let mut web = vec![0; web_size as usize];
        state
            .read_exact(&mut web)
            .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;

        let decoder = XzDecoder::new(Cursor::new(web));
        let web = Archive::new(decoder);

        Ok(Self { metadata, web })
    }
}
