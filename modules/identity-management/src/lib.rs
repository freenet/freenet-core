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
        ctx: &mut DelegateCtx,
        params: Parameters<'static>,
        _origin: Option<MessageOrigin>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        let params = IdentityParams::try_from(params)?;
        let secret_key =
            serde_json::to_vec(&params).map_err(|e| DelegateError::Deser(format!("{e}")))?;
        match message {
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage {
                payload,
                processed: false,
                ..
            }) => {
                let msg = IdentityMsg::try_from(&*payload)?;
                match msg {
                    IdentityMsg::Init => {
                        #[cfg(feature = "contract")]
                        {
                            freenet_stdlib::log::info(&format!(
                                "initialize secret {}",
                                params.as_secret_id()
                            ));
                        }
                        let default_value = serde_json::to_vec(&IdentityManagement::default())
                            .map_err(|e| DelegateError::Deser(format!("{e}")))?;
                        ctx.set_secret(&secret_key, &default_value);
                        Ok(vec![])
                    }
                    IdentityMsg::CreateIdentity { alias, key, extra } => {
                        #[cfg(feature = "contract")]
                        {
                            freenet_stdlib::log::info(&format!(
                                "create alias new {alias} for {}",
                                params.as_secret_id()
                            ));
                        }
                        let value = ctx
                            .get_secret(&secret_key)
                            .ok_or_else(|| DelegateError::Other("secret not found".into()))?;
                        let mut manager = IdentityManagement::try_from(value.as_slice())?;
                        manager.identities.insert(alias, AliasInfo { key, extra });
                        let updated = serde_json::to_vec(&manager)
                            .map_err(|e| DelegateError::Deser(format!("{e}")))?;
                        ctx.set_secret(&secret_key, &updated);
                        Ok(vec![])
                    }
                    IdentityMsg::DeleteIdentity { alias } => {
                        let value = ctx
                            .get_secret(&secret_key)
                            .ok_or_else(|| DelegateError::Other("secret not found".into()))?;
                        let mut manager = IdentityManagement::try_from(value.as_slice())?;
                        manager.identities.remove(alias.as_str());
                        let updated = serde_json::to_vec(&manager)
                            .map_err(|e| DelegateError::Deser(format!("{e}")))?;
                        ctx.set_secret(&secret_key, &updated);
                        Ok(vec![])
                    }
                }
            }
            _ => Err(DelegateError::Other("unexpected message type".into())),
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
