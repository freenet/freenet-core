use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

/// Data that forms part of a contract or a delegate along with the WebAssembly code.
///
/// This is supplied to the contract as a parameter to the contract's functions. Parameters are
/// typically be used to configure a contract, much like the parameters of a constructor function.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde_as]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct Parameters<'a>(
    // TODO: conver this to Arc<u8> instead
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    Cow<'a, [u8]>,
);

impl<'a> Parameters<'a> {
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
}

impl<'a> From<Vec<u8>> for Parameters<'a> {
    fn from(data: Vec<u8>) -> Self {
        Parameters(Cow::from(data))
    }
}

impl<'a> From<&'a [u8]> for Parameters<'a> {
    fn from(s: &'a [u8]) -> Self {
        Parameters(Cow::from(s))
    }
}

impl<'a> AsRef<[u8]> for Parameters<'a> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}
