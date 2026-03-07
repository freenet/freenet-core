use std::borrow::Cow;

use serde::{Deserialize, Deserializer, Serialize};
use serde_with::serde_as;

/// Data that forms part of a contract or a delegate along with the WebAssembly code.
///
/// This is supplied to the contract as a parameter to the contract's functions. Parameters are
/// typically be used to configure a contract, much like the parameters of a constructor function.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde_as]
#[cfg_attr(
    any(feature = "testing", all(test, any(unix, windows))),
    derive(arbitrary::Arbitrary)
)]
pub struct Parameters<'a>(
    // TODO: conver this to Arc<u8> instead
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    Cow<'a, [u8]>,
);

impl Parameters<'_> {
    /// Gets the number of bytes of data stored in the `Parameters`.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Returns the bytes of parameters.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_owned()
    }

    /// Copies the data if not owned and returns an owned version of self.
    pub fn into_owned(self) -> Parameters<'static> {
        let data: Cow<'static, _> = Cow::from(self.0.into_owned());
        Parameters(data)
    }

    pub fn deser_params<'de, D>(deser: D) -> Result<Parameters<'static>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: Parameters<'de> = Deserialize::deserialize(deser)?;
        Ok(data.into_owned())
    }
}

impl From<Vec<u8>> for Parameters<'_> {
    fn from(data: Vec<u8>) -> Self {
        Parameters(Cow::from(data))
    }
}

impl<'a> From<&'a [u8]> for Parameters<'a> {
    fn from(s: &'a [u8]) -> Self {
        Parameters(Cow::from(s))
    }
}

impl AsRef<[u8]> for Parameters<'_> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}
