use std::collections::HashMap;

use freenet_stdlib::prelude::*;
use p384::{FieldBytes, Scalar, SecretKey};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Serialize, Deserialize)]
pub struct IdentityParams {
    #[serde(serialize_with = "IdentityParams::pk_serialize")]
    #[serde(deserialize_with = "IdentityParams::pk_deserialize")]
    pub secret_key: SecretKey,
}

impl IdentityParams {
    pub fn as_secret_id(&self) -> SecretsId {
        SecretsId::new(serde_json::to_vec(self).unwrap())
    }
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
        let deser: Self = Self::try_from(value.as_ref())?;
        Ok(deser)
    }
}

impl TryFrom<&[u8]> for IdentityParams {
    type Error = DelegateError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let deser: Self =
            serde_json::from_slice(value).map_err(|e| DelegateError::Deser(format!("{e}")))?;
        Ok(deser)
    }
}

impl TryFrom<IdentityParams> for Parameters<'static> {
    type Error = DelegateError;

    fn try_from(value: IdentityParams) -> Result<Self, Self::Error> {
        let ser = serde_json::to_vec(&value).map_err(|e| DelegateError::Deser(format!("{e}")))?;
        Ok(Parameters::from(ser))
    }
}

type Key = Vec<u8>;
type Alias = String;
type Extra = String;

#[derive(Deserialize, Serialize, PartialEq, Eq, Debug)]
pub struct AliasInfo {
    pub key: Key,
    pub extra: Option<Extra>,
}

#[derive(Deserialize, Serialize, Default, Debug)]
pub struct IdentityManagement {
    identities: HashMap<Alias, AliasInfo>,
}

impl IdentityManagement {
    pub fn is_empty(&self) -> bool {
        self.identities.is_empty()
    }

    pub fn get_info(&self) -> impl Iterator<Item = (&Alias, &AliasInfo)> {
        self.identities.iter()
    }

    pub fn into_info(self) -> impl Iterator<Item = (Alias, AliasInfo)> {
        self.identities.into_iter()
    }

    pub fn remove(&mut self, alias: &str) -> Option<AliasInfo> {
        self.identities.remove(alias)
    }
}

impl TryFrom<&[u8]> for IdentityManagement {
    type Error = DelegateError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let deser: Self = serde_json::from_slice(value).unwrap();
        Ok(deser)
    }
}

#[delegate]
impl DelegateInterface for IdentityManagement {
    fn process(
        params: Parameters<'static>,
        _attested: Option<&'static [u8]>,
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
                    IdentityMsg::CreateIdentity { alias, key, extra } => {
                        #[cfg(feature = "contract")]
                        {
                            freenet_stdlib::log::info(&format!(
                                "create alias new {alias} for {}",
                                params.as_secret_id()
                            ));
                        }
                        serde_json::to_vec(&IdentityMsg::CreateIdentity { alias, key, extra })
                            .unwrap()
                    }
                    IdentityMsg::DeleteIdentity { alias } => {
                        serde_json::to_vec(&IdentityMsg::DeleteIdentity { alias }).unwrap()
                    }
                    IdentityMsg::Init => {
                        #[cfg(feature = "contract")]
                        {
                            freenet_stdlib::log::info(&format!(
                                "initialize secret {}",
                                params.as_secret_id()
                            ));
                        }
                        let set_secret = OutboundDelegateMsg::SetSecretRequest(SetSecretRequest {
                            key: params.as_secret_id(),
                            value: Some(
                                serde_json::to_vec(&IdentityManagement::default()).unwrap(),
                            ),
                        });
                        return Ok(vec![set_secret]);
                    }
                };
                let context: DelegateContext = DelegateContext::new(action);
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
                #[cfg(feature = "contract")]
                {
                    freenet_stdlib::log::info(&format!(
                        "got request for {}",
                        params.as_secret_id()
                    ));
                }
                if !context.as_ref().is_empty() {
                    let context = IdentityMsg::try_from(context.as_ref()).unwrap();
                    let mut manager = IdentityManagement::try_from(&*value)?;
                    match context {
                        IdentityMsg::CreateIdentity { alias, key, extra } => {
                            manager.identities.insert(alias, AliasInfo { key, extra });
                        }
                        IdentityMsg::DeleteIdentity { alias } => {
                            manager.identities.remove(alias.as_str());
                        }
                        IdentityMsg::Init => {
                            unreachable!()
                        }
                    };
                    let outbound = OutboundDelegateMsg::SetSecretRequest(SetSecretRequest {
                        key: params.as_secret_id(),
                        value: Some(serde_json::to_vec(&manager).unwrap()),
                    });
                    Ok(vec![outbound])
                } else {
                    Err(DelegateError::Other("invalid request".into()))
                }
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum IdentityMsg {
    Init,
    CreateIdentity {
        alias: String,
        key: Key,
        extra: Option<String>,
    },
    DeleteIdentity {
        alias: String,
    },
}

impl TryFrom<&[u8]> for IdentityMsg {
    type Error = DelegateError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let msg = serde_json::from_slice(value).unwrap();
        Ok(msg)
    }
}

impl TryFrom<&IdentityMsg> for Vec<u8> {
    type Error = DelegateError;

    fn try_from(value: &IdentityMsg) -> Result<Self, Self::Error> {
        let msg = serde_json::to_vec(&value).unwrap();
        Ok(msg)
    }
}
