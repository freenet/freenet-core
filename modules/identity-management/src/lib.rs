use std::collections::HashMap;

use locutus_stdlib::prelude::*;
use p384::{FieldBytes, Scalar, SecretKey};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

type Key = Vec<u8>;
type Alias = String;

#[derive(Deserialize, Serialize)]
struct IdentityManagement {
    identities: HashMap<Alias, Key>,
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
                payload,
                processed: false,
                ..
            }) => {
                let msg = IdentityMsg::try_from(&*payload)?;
                let action = match msg {
                    IdentityMsg::CreateIdentity { alias, key } => {
                        serde_json::to_vec(&IdentityMsg::CreateIdentity { alias, key }).unwrap()
                    }
                    IdentityMsg::DeleteIdentity { alias } => {
                        serde_json::to_vec(&IdentityMsg::DeleteIdentity { alias }).unwrap()
                    }
                };
                let context = DelegateContext::new(action);
                let get_secret = OutboundDelegateMsg::GetSecretRequest(GetSecretRequest {
                    key: SecretsId::new(serde_json::to_vec(&params).unwrap()),
                    context,
                    processed: false,
                });
                Ok(vec![get_secret])
            }
            InboundDelegateMsg::GetSecretResponse(GetSecretResponse {
                value: Some(value),
                context,
                ..
            }) => {
                if !context.as_ref().is_empty() {
                    let context = IdentityMsg::try_from(context.as_ref()).unwrap();
                    let mut manager = IdentityManagement::try_from(&*value)?;
                    match context {
                        IdentityMsg::CreateIdentity { alias, key } => {
                            manager.identities.insert(alias, key);
                        }
                        IdentityMsg::DeleteIdentity { alias } => {
                            manager.identities.remove(&alias);
                        }
                    };
                    let outbound = OutboundDelegateMsg::SetSecretRequest(SetSecretRequest {
                        key: SecretsId::new(serde_json::to_vec(&params).unwrap()),
                        value: Some(serde_json::to_vec(&manager).unwrap()),
                    });
                    Ok(vec![outbound])
                } else {
                    todo!()
                }
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum IdentityMsg {
    CreateIdentity { alias: String, key: Key },
    DeleteIdentity { alias: String },
}

impl TryFrom<&[u8]> for IdentityMsg {
    type Error = DelegateError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let msg = serde_json::from_slice(value).unwrap();
        Ok(msg)
    }
}
