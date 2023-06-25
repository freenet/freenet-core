use locutus_stdlib::prelude::*;
use p384::{FieldBytes, Scalar, SecretKey};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Deserialize, Serialize)]
struct IdentityManagement;

#[derive(Serialize, Deserialize)]
pub struct IdentityParams {
    #[serde(serialize_with = "IdentityParams::pk_serialize")]
    #[serde(deserialize_with = "IdentityParams::pk_deserialize")]
    secret_key: SecretKey,
}

impl IdentityParams {
    fn pk_deserialize<'de, D>(deser: D) -> Result<SecretKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let scalar = <Scalar as Deserialize>::deserialize(deser)?;
        Ok(SecretKey::from_bytes(&FieldBytes::from(scalar)).unwrap())
    }

    fn pk_serialize<S>(key: &SecretKey, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let scalar: Scalar = key.as_scalar_primitive().into();
        scalar.serialize(ser)
    }
}

impl TryFrom<Parameters<'_>> for IdentityParams {
    type Error = DelegateError;

    fn try_from(value: Parameters<'_>) -> Result<Self, Self::Error> {
        let deser: Self = serde_json::from_slice(value.as_ref()).unwrap();
        Ok(deser)
    }
}

impl TryFrom<&[u8]> for IdentityManagement {
    type Error = DelegateError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let deser: Self = serde_json::from_slice(value).unwrap();
        Ok(deser)
    }
}

impl DelegateInterface for IdentityManagement {
    fn process(
        params: Parameters<'static>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        let params = IdentityParams::try_from(params)?;
        match message {
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage {
                app,
                payload,
                processed: false,
                ..
            }) => {}
            InboundDelegateMsg::GetSecretResponse(GetSecretResponse {
                value: Some(value), ..
            }) => {
                let manager = IdentityManagement::try_from(&*value)?;
            }
            InboundDelegateMsg::UserResponse(_) => todo!(),
            _ => unreachable!(),
        }
        todo!()
    }
}
