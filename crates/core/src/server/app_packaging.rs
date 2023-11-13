//! Helper functions and types for dealing with HTTP gateway compatible contracts.
use std::{
    io::{Cursor, Read},
    path::Path,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tar::{Archive, Builder};
use xz2::read::{XzDecoder, XzEncoder};

use crate::DynError;

#[derive(Debug, thiserror::Error)]
pub enum WebContractError {
    #[error("unpacking error: {0}")]
    UnpackingError(DynError),
    #[error("{0}")]
    StoringError(std::io::Error),
    #[error("file not found: {0}")]
    FileNotFound(String),
}

#[non_exhaustive]
pub struct WebApp {
    pub metadata: Vec<u8>,
    pub web: Vec<u8>,
}

impl WebApp {
    pub fn from_data(
        metadata: Vec<u8>,
        web: Builder<Cursor<Vec<u8>>>,
    ) -> Result<Self, WebContractError> {
        let buf = web.into_inner().unwrap().into_inner();
        let mut encoder = XzEncoder::new(Cursor::new(buf), 6);
        let mut compressed = vec![];
        encoder.read_to_end(&mut compressed).unwrap();
        Ok(Self {
            metadata,
            web: compressed,
        })
    }

    pub fn pack(mut self) -> std::io::Result<Vec<u8>> {
        let mut output = Vec::with_capacity(
            self.metadata.len() + self.web.len() + (std::mem::size_of::<u64>() * 2),
        );
        output.write_u64::<BigEndian>(self.metadata.len() as u64)?;
        output.append(&mut self.metadata);
        output.write_u64::<BigEndian>(self.web.len() as u64)?;
        output.append(&mut self.web);
        Ok(output)
    }

    pub fn unpack(&mut self, dst: impl AsRef<Path>) -> Result<(), WebContractError> {
        let mut decoded_web = self.decode_web();
        decoded_web
            .unpack(dst)
            .map_err(WebContractError::StoringError)?;
        Ok(())
    }

    pub fn get_file(&mut self, path: &str) -> Result<Vec<u8>, WebContractError> {
        let mut decoded_web = self.decode_web();
        for e in decoded_web
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

    fn decode_web(&self) -> Archive<XzDecoder<&[u8]>> {
        let decoder = XzDecoder::new(self.web.as_slice());
        Archive::new(decoder)
    }
}

impl<'a> TryFrom<&'a [u8]> for WebApp {
    type Error = WebContractError;

    fn try_from(state: &'a [u8]) -> Result<Self, Self::Error> {
        const MAX_METADATA_SIZE: u64 = 1024;
        const MAX_WEB_SIZE: u64 = 1024 * 1024 * 100;
        // Decompose the state and extract the compressed web interface
        let mut state = Cursor::new(state);

        let metadata_size = state
            .read_u64::<BigEndian>()
            .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;
        if metadata_size > MAX_METADATA_SIZE {
            return Err(WebContractError::UnpackingError(
                format!("Exceeded metadata size of 1kB: {} bytes", metadata_size).into(),
            ));
        }
        let mut metadata = vec![0; metadata_size as usize];
        state
            .read_exact(&mut metadata)
            .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;

        let web_size = state
            .read_u64::<BigEndian>()
            .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;
        if web_size > MAX_WEB_SIZE {
            return Err(WebContractError::UnpackingError(
                format!("Exceeded packed web size of 100MB: {} bytes", web_size).into(),
            ));
        }
        let mut web = vec![0; web_size as usize];
        state
            .read_exact(&mut web)
            .map_err(|e| WebContractError::UnpackingError(Box::new(e)))?;

        Ok(Self { metadata, web })
    }
}
