use std::collections::{HashMap, HashSet};
use std::fmt::Display;

use chrono::{DateTime, Utc};
use locutus_aft_interface::{
    InvalidReason as TokenInvalidReason, Tier, TokenAllocationRecord, TokenAssignment,
    TokenAssignmentHash,
};
use locutus_stdlib::prelude::{blake2::Digest, *};
use rsa::{
    pkcs1v15::{SigningKey, VerifyingKey},
    sha2::Sha256,
    signature::{Signer, Verifier},
    RsaPrivateKey, RsaPublicKey,
};
use serde::{Deserialize, Serialize};

/// Sign this byte array and include the signature in the `inbox_signature` so this inbox can be verified on updates.
const STATE_UPDATE: &[u8; 8] = &[168, 7, 13, 64, 168, 123, 142, 215];

#[derive(Serialize, Deserialize)]
pub struct InboxParams {
    // The public key of the inbox owner message.
    pub pub_key: RsaPublicKey,
}

impl TryFrom<InboxParams> for Parameters<'_> {
    type Error = serde_json::Error;

    fn try_from(value: InboxParams) -> Result<Self, Self::Error> {
        serde_json::to_vec(&value).map(Into::into)
    }
}

impl TryFrom<Parameters<'static>> for InboxParams {
    type Error = ContractError;
    fn try_from(params: Parameters<'static>) -> Result<Self, Self::Error> {
        serde_json::from_slice(params.as_ref())
            .map_err(|err| ContractError::Deser(format!("{err}")))
    }
}

type Signature = Box<[u8]>;

#[derive(Serialize, Deserialize)]
pub enum UpdateInbox {
    AddMessages {
        messages: Vec<Message>,
    },
    RemoveMessages {
        signature: Signature,
        ids: Vec<TokenAssignmentHash>,
    },
    ModifySettings {
        signature: Signature,
        settings: InboxSettings,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InboxSettings {
    pub minimum_tier: Tier,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Vec::default")]
    pub private: EncryptedContent,
}

impl TryFrom<StateDelta<'static>> for UpdateInbox {
    type Error = ContractError;
    fn try_from(state: StateDelta<'static>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&state).map_err(|err| ContractError::Deser(format!("{err}")))
    }
}

/// Requires a shared key between both peers to decrypt the secrets.
/// The inbox contract does not need to be aware of the content.
type EncryptedContent = Vec<u8>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
    pub content: EncryptedContent,
    pub token_assignment: TokenAssignment,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Inbox {
    pub messages: Vec<Message>,
    pub last_update: DateTime<Utc>,
    pub settings: InboxSettings,
    inbox_signature: Signature,
}

enum VerificationError {
    MissingContracts(Vec<ContractInstanceId>),
    TokenAssignmentMismatch,
    InvalidToken(TokenInvalidReason),
    InvalidMessageHash,
    WrongSignature,
}

impl From<VerificationError> for ContractError {
    fn from(_err: VerificationError) -> Self {
        ContractError::InvalidUpdate
    }
}

impl Display for VerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VerificationError::MissingContracts(ids) => {
                write!(
                    f,
                    "Missing contracts: {}",
                    ids.iter()
                        .map(|c| format!("{c}"))
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(",")
                )
            }
            VerificationError::TokenAssignmentMismatch => {
                write!(f, "Token assignment mismatch")
            }
            VerificationError::InvalidToken(reason) => {
                write!(f, "Invalid token: {reason}")
            }
            VerificationError::InvalidMessageHash => {
                write!(f, "Invalid message hash")
            }
            VerificationError::WrongSignature => {
                write!(f, "Wrong signature")
            }
        }
    }
}

impl Inbox {
    #[cfg(feature = "wasmbind")]
    pub fn new(key: &RsaPrivateKey, settings: InboxSettings, messages: Vec<Message>) -> Self {
        let inbox_signature = Self::sign(key);
        Self {
            settings,
            messages,
            last_update: Utc::now(),
            inbox_signature,
        }
    }

    pub fn sign(key: &RsaPrivateKey) -> Signature {
        let signing_key = SigningKey::<Sha256>::new_with_prefix(key.clone());
        signing_key.sign(STATE_UPDATE).into()
    }

    fn verify(&self, params: &InboxParams) -> Result<(), VerificationError> {
        let verifying_key = VerifyingKey::<Sha256>::new_with_prefix(params.pub_key.clone());
        verifying_key
            .verify(
                STATE_UPDATE,
                &rsa::pkcs1v15::Signature::from(self.inbox_signature.clone()),
            )
            .map_err(|_e| VerificationError::WrongSignature)?;
        Ok(())
    }

    fn add_messages(
        &mut self,
        allocation_records: &HashMap<ContractInstanceId, TokenAllocationRecord>,
        messages: Vec<Message>,
    ) -> Result<(), VerificationError> {
        // FIXME: make sure we are not re-adding old messages by verifying the time is more recent
        // than last updated
        for message in messages {
            let records = allocation_records
                .get(&message.token_assignment.token_record)
                .ok_or_else(|| {
                    VerificationError::MissingContracts(vec![message.token_assignment.token_record])
                })?;
            if !records.assignment_exists(&message.token_assignment) {
                return Err(VerificationError::TokenAssignmentMismatch);
            }
            self.add_message(message)?;
        }
        Ok(())
    }

    fn verify_messages(
        &self,
        allocation_records: &HashMap<ContractInstanceId, TokenAllocationRecord>,
    ) -> Result<(), VerificationError> {
        let mut some_missing = false;
        let mut missing = vec![];
        for message in &self.messages {
            let Some(records) = allocation_records.get(&message.token_assignment.token_record) else {
                missing.push(message.token_assignment.token_record);
                some_missing = true;
                continue;
            };
            if some_missing {
                continue;
            }
            if !records.assignment_exists(&message.token_assignment) {
                return Err(VerificationError::TokenAssignmentMismatch);
            }
            let verifying_key =
                VerifyingKey::<Sha256>::from(message.token_assignment.generator.clone());
            message
                .token_assignment
                .is_valid(&verifying_key)
                .map_err(VerificationError::InvalidToken)?;
        }
        if !missing.is_empty() {
            return Err(VerificationError::MissingContracts(missing));
        }
        Ok(())
    }

    fn add_message(&mut self, message: Message) -> Result<(), VerificationError> {
        let verifying_key =
            VerifyingKey::<Sha256>::from(message.token_assignment.generator.clone());
        message
            .token_assignment
            .is_valid(&verifying_key)
            .map_err(|e| {
                #[cfg(target_family = "wasm")]
                {
                    match &e {
                        TokenInvalidReason::SignatureMismatch => {
                            use rsa::pkcs8::EncodePublicKey;
                            let pk = message
                                .token_assignment
                                .generator
                                .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
                                .unwrap()
                                .split_whitespace()
                                .collect::<String>();
                            locutus_stdlib::log::info(&format!("veryifying key inbox: `{pk}`"));
                        }
                        _ => {}
                    }
                }
                VerificationError::InvalidToken(e)
            })?;
        self.messages.push(message);
        Ok(())
    }

    fn remove_messages(&mut self, messages: HashSet<TokenAssignmentHash>) {
        self.messages
            .retain(|m| !messages.contains(&m.token_assignment.assignment_hash));
    }

    fn summarize(self) -> Result<StateSummary<'static>, ContractError> {
        let messages = self
            .messages
            .into_iter()
            .map(
                |Message {
                     token_assignment, ..
                 }| token_assignment.assignment_hash,
            )
            .collect::<HashSet<_>>();
        let serialized = serde_json::to_vec(&InboxSummary(messages))
            .map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(StateSummary::from(serialized))
    }

    fn merge(&mut self, other: Self) -> Result<(), ContractError> {
        if self.messages.is_empty() && self.last_update < other.last_update {
            for m in other.messages {
                self.add_message(m)?;
            }
        }
        Ok(())
    }

    fn delta(self, InboxSummary(messages): InboxSummary) -> Inbox {
        let delta = self
            .messages
            .into_iter()
            .filter(|m| !messages.contains(&m.token_assignment.assignment_hash))
            .collect();
        Inbox {
            messages: delta,
            last_update: self.last_update,
            settings: self.settings,
            inbox_signature: self.inbox_signature,
        }
    }
}

impl TryFrom<&'_ State<'static>> for Inbox {
    type Error = ContractError;
    fn try_from(state: &State<'static>) -> Result<Self, Self::Error> {
        serde_json::from_slice(state).map_err(|err| ContractError::Deser(format!("{err}")))
    }
}

impl TryFrom<Inbox> for StateDelta<'static> {
    type Error = ContractError;
    fn try_from(value: Inbox) -> Result<Self, Self::Error> {
        let serialized =
            serde_json::to_vec(&value).map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(serialized.into())
    }
}

#[derive(Serialize, Deserialize)]
struct InboxSummary(HashSet<TokenAssignmentHash>);

impl TryFrom<StateSummary<'static>> for InboxSummary {
    type Error = ContractError;
    fn try_from(summary: StateSummary<'static>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&summary).map_err(|err| ContractError::Deser(format!("{err}")))
    }
}

/// Whether this messages can be added/removed from the inbox.
fn can_modify_inbox<'a>(
    params: &InboxParams,
    signature: &Signature,
    ids: impl Iterator<Item = &'a TokenAssignmentHash>,
) -> Result<(), ContractError> {
    let mut signed = Vec::with_capacity(ids.size_hint().0 * 32);
    for hash in ids {
        signed.extend(hash);
    }
    let verifying_key = VerifyingKey::<Sha256>::from(params.pub_key.clone());
    verifying_key
        .verify(signed.as_ref(), &signature.clone().into())
        .map_err(|_err| ContractError::InvalidUpdate)?;
    Ok(())
}

fn can_update_settings(
    params: &InboxParams,
    signature: &Signature,
    settings: &InboxSettings,
) -> Result<(), ContractError> {
    let serialized =
        serde_json::to_vec(settings).map_err(|e| ContractError::Deser(format!("{e}")))?;
    let verifying_key = VerifyingKey::<Sha256>::from(params.pub_key.clone());
    verifying_key
        .verify(&serialized, &signature.clone().into())
        .map_err(|_err| ContractError::InvalidUpdate)?;
    Ok(())
}

#[cfg(feature = "contract")]
#[contract]
impl ContractInterface for Inbox {
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let inbox = Inbox::try_from(&state)?;
        let params = InboxParams::try_from(parameters)?;
        inbox.verify(&params)?;

        let mut missing_related = vec![];
        let mut allocation_records = HashMap::new();
        for (contract_id, state) in related.states() {
            let Some(state) = state else {
                missing_related.push(contract_id);
                continue;
            };
            let token_record = TokenAllocationRecord::try_from(state)?;
            allocation_records.insert(contract_id, token_record);
        }

        if !missing_related.is_empty() {
            return Ok(ValidateResult::RequestRelated(missing_related));
        }

        match inbox.verify_messages(&allocation_records) {
            Ok(_) => Ok(ValidateResult::Valid),
            Err(VerificationError::MissingContracts(ids)) => {
                Ok(ValidateResult::RequestRelated(ids))
            }
            Err(_err) => Ok(ValidateResult::Invalid),
        }
    }

    fn validate_delta(
        _parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        Ok(UpdateInbox::try_from(delta).ok().is_some())
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        updates: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        // fixme: take care of race condition between token alloc record and the assignment
        let mut inbox = Inbox::try_from(&state)?;
        let params = InboxParams::try_from(parameters)?;
        inbox.verify(&params)?;
        let mut missing_related = vec![];
        let mut new_messages = vec![];
        let mut rm_messages = HashSet::new();
        let mut allocation_records = HashMap::new();
        for update in updates {
            match update {
                UpdateData::State(state) => {
                    let full_inbox = Inbox::try_from(&state)?;
                    inbox.merge(full_inbox)?;
                }
                UpdateData::Delta(d) => match UpdateInbox::try_from(d)? {
                    UpdateInbox::AddMessages { mut messages } => {
                        for m in &messages {
                            missing_related.push(m.token_assignment.token_record);
                        }
                        new_messages.append(&mut messages);
                    }
                    UpdateInbox::RemoveMessages { signature, ids } => {
                        can_modify_inbox(&params, &signature, ids.iter())?;
                        rm_messages.extend(ids);
                    }
                    UpdateInbox::ModifySettings {
                        settings,
                        signature,
                    } => {
                        can_update_settings(&params, &signature, &settings)?;
                        inbox.settings = settings;
                    }
                },
                UpdateData::RelatedState { related_to, state } => {
                    let token_record = TokenAllocationRecord::try_from(state)?;
                    allocation_records.insert(related_to, token_record);
                }
                _ => unreachable!(),
            }
        }

        let missing_related: Vec<_> = missing_related
            .into_iter()
            .filter_map(|missing| {
                (!allocation_records.contains_key(&missing)).then(|| RelatedContract {
                    contract_instance_id: missing,
                    mode: RelatedMode::StateOnce,
                })
            })
            .collect();

        if missing_related.is_empty() {
            inbox
                .add_messages(&allocation_records, new_messages)
                .map_err(|err| ContractError::Other(format!("{err}")))?;
            inbox.remove_messages(rm_messages);
            // FIXME: uncomment next line, right now it pulls the `time` dep on the web UI if we enable which is not what we want
            //inbox.last_update = locutus_stdlib::time::now();
            let serialized =
                serde_json::to_vec(&inbox).map_err(|err| ContractError::Deser(format!("{err}")))?;
            Ok(UpdateModification::valid(serialized.into()))
        } else {
            Ok(UpdateModification::requires(missing_related))
        }
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let inbox = Inbox::try_from(&state)?;
        inbox.summarize()
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        // wrong summary representations
        let inbox = Inbox::try_from(&state)?;
        let summary = InboxSummary::try_from(summary)?;
        let delta = inbox.delta(summary);
        delta.try_into()
    }
}

#[cfg(all(feature = "contract", test))]
mod tests {
    use super::*;
    use rsa::{pkcs1::DecodeRsaPrivateKey, Pkcs1v15Sign, RsaPrivateKey};

    #[test]
    fn validate_test() -> Result<(), Box<dyn std::error::Error>> {
        let private_key = RsaPrivateKey::from_pkcs1_pem(include_str!(
            "../../../web/examples/rsa4096-id-1-priv.pem"
        ))
        .unwrap();
        let public_key = private_key.to_public_key();

        let params: Parameters = InboxParams {
            pub_key: public_key.clone(),
        }
        .try_into()
        .map_err(|e| format!("{e}"))
        .unwrap();

        let digest = Sha256::digest(STATE_UPDATE).to_vec();
        let signature = private_key
            .sign(Pkcs1v15Sign::new::<Sha256>(), &digest)
            .unwrap();

        let state_bytes = format!(
            r#"{{
            "messages": [],
            "last_update": "2022-05-10T00:00:00Z",
            "settings": {{
                "minimum_tier": "Day1",
                "private": []
            }},
            "inbox_signature": {}
        }}"#,
            serde_json::to_string(&signature).unwrap()
        )
        .as_bytes()
        .to_vec();

        let verifying_key = VerifyingKey::<Sha256>::new_with_prefix(public_key);
        let sign = &rsa::pkcs1v15::Signature::try_from(signature.as_slice()).unwrap();

        match verifying_key.verify(STATE_UPDATE, sign) {
            Ok(_) => println!("successful verification"),
            Err(e) => println!("verification error: {:?}", e),
        };

        let is_valid = Inbox::validate_state(params, State::from(state_bytes), Default::default())?;
        assert!(is_valid == ValidateResult::Valid);
        Ok(())
    }

    #[test]
    fn update_test() -> Result<(), Box<dyn std::error::Error>> {
        let private_key = RsaPrivateKey::from_pkcs1_pem(include_str!(
            "../../../web/examples/rsa4096-id-1-priv.pem"
        ))
        .unwrap();
        let public_key = private_key.to_public_key();

        let params: Parameters = InboxParams {
            pub_key: public_key.clone(),
        }
        .try_into()
        .map_err(|e| format!("{e}"))
        .unwrap();

        let digest = Sha256::digest(STATE_UPDATE).to_vec();
        let signature = private_key
            .sign(Pkcs1v15Sign::new::<Sha256>(), &digest)
            .unwrap();

        let state_bytes = format!(
            r#"{{
            "messages": [],
            "last_update": "2022-05-10T00:00:00Z",
            "settings": {{
                "minimum_tier": "Day1",
                "private": []
            }},
            "inbox_signature": {}
        }}"#,
            serde_json::to_string(&signature).unwrap()
        )
        .as_bytes()
        .to_vec();

        let update = UpdateData::Delta(
            vec![123, 34, 65, 100, 100, 77, 101, 115, 115, 97, 103, 101, 115, 34, 58, 123, 34, 109,
                101, 115, 115, 97, 103, 101, 115, 34, 58, 91, 123, 34, 99, 111, 110, 116, 101, 110,
                116, 34, 58, 91, 50, 50, 48, 44, 50, 50, 50, 44, 53, 50, 44, 49, 57, 54, 44, 49,
                54, 55, 44, 49, 49, 57, 44, 49, 55, 57, 44, 49, 50, 55, 44, 50, 52, 54, 44, 49, 53,
                52, 44, 49, 52, 52, 44, 50, 51, 48, 44, 49, 55, 57, 44, 49, 54, 52, 44, 50, 53, 49,
                44, 56, 51, 44, 51, 50, 44, 55, 50, 44, 49, 57, 51, 44, 52, 55, 44, 49, 51, 52, 44,
                49, 53, 53, 44, 52, 48, 44, 49, 56, 44, 53, 50, 44, 54, 54, 44, 50, 50, 48, 44, 50,
                53, 44, 49, 51, 54, 44, 55, 53, 44, 49, 54, 52, 44, 49, 54, 48, 44, 50, 56, 44, 50,
                49, 48, 44, 50, 48, 48, 44, 54, 49, 44, 49, 48, 50, 44, 50, 52, 57, 44, 49, 51, 56,
                44, 56, 48, 44, 49, 57, 48, 44, 49, 50, 52, 44, 49, 57, 52, 44, 52, 55, 44, 53, 56,
                44, 55, 49, 44, 49, 53, 49, 44, 50, 48, 56, 44, 49, 52, 51, 44, 49, 53, 55, 44, 49,
                50, 56, 44, 49, 53, 50, 44, 50, 48, 57, 44, 50, 51, 50, 44, 49, 56, 57, 44, 49, 57,
                57, 44, 53, 54, 44, 51, 50, 44, 49, 51, 48, 44, 56, 50, 44, 50, 50, 44, 50, 49, 56,
                44, 49, 52, 56, 44, 56, 54, 44, 49, 50, 50, 44, 50, 51, 49, 44, 52, 50, 44, 50, 51,
                57, 44, 54, 52, 44, 50, 51, 56, 44, 49, 49, 49, 44, 50, 50, 51, 44, 50, 44, 49, 50,
                55, 44, 50, 49, 48, 44, 49, 57, 56, 44, 49, 52, 56, 44, 49, 55, 49, 44, 49, 56, 54,
                44, 57, 56, 44, 56, 57, 44, 49, 48, 55, 44, 49, 53, 49, 44, 50, 52, 52, 44, 50, 52,
                57, 44, 49, 49, 55, 44, 49, 54, 55, 44, 49, 52, 49, 44, 49, 50, 51, 44, 49, 51, 56,
                44, 54, 54, 44, 50, 51, 49, 44, 50, 51, 54, 44, 50, 49, 52, 44, 49, 50, 50, 44, 50,
                49, 55, 44, 57, 55, 44, 50, 53, 51, 44, 50, 51, 54, 44, 49, 57, 56, 44, 50, 51, 44,
                50, 48, 55, 44, 50, 48, 50, 44, 50, 49, 49, 44, 49, 48, 52, 44, 49, 54, 50, 44, 55,
                54, 44, 49, 52, 48, 44, 49, 52, 53, 44, 55, 53, 44, 56, 44, 55, 54, 44, 50, 52, 55,
                44, 49, 49, 49, 44, 49, 49, 56, 44, 52, 53, 44, 49, 49, 49, 44, 52, 48, 44, 49, 53,
                57, 44, 51, 49, 44, 52, 50, 44, 49, 50, 52, 44, 50, 52, 44, 49, 50, 44, 52, 51, 44,
                49, 50, 44, 49, 44, 51, 56, 44, 50, 50, 53, 44, 49, 55, 54, 44, 49, 55, 54, 44, 56,
                49, 44, 54, 44, 50, 50, 48, 44, 49, 56, 44, 50, 50, 50, 44, 53, 56, 44, 52, 57, 44,
                49, 56, 49, 44, 50, 52, 51, 44, 53, 56, 44, 52, 57, 44, 49, 55, 56, 44, 50, 51, 49,
                44, 57, 53, 44, 50, 49, 57, 44, 49, 50, 48, 44, 51, 54, 44, 49, 51, 52, 44, 49, 57,
                53, 44, 50, 50, 57, 44, 50, 51, 44, 49, 50, 55, 44, 50, 53, 50, 44, 50, 51, 49, 44,
                50, 49, 52, 44, 49, 57, 54, 44, 53, 54, 44, 49, 50, 49, 44, 54, 53, 44, 50, 51, 55,
                44, 57, 53, 44, 50, 49, 52, 44, 50, 49, 49, 44, 52, 50, 44, 49, 56, 56, 44, 50, 49,
                52, 44, 49, 48, 53, 44, 49, 55, 50, 44, 49, 53, 54, 44, 57, 56, 44, 50, 51, 49, 44,
                49, 51, 54, 44, 49, 49, 51, 44, 49, 56, 57, 44, 57, 49, 44, 53, 50, 44, 50, 53, 53,
                44, 51, 54, 44, 49, 50, 50, 44, 49, 54, 52, 44, 54, 56, 44, 50, 48, 56, 44, 49, 50,
                56, 44, 54, 57, 44, 49, 52, 51, 44, 49, 56, 48, 44, 49, 53, 54, 44, 49, 56, 57, 44,
                50, 52, 56, 44, 49, 57, 56, 44, 49, 44, 50, 51, 56, 44, 50, 53, 51, 44, 49, 53, 50,
                44, 49, 51, 52, 44, 49, 55, 48, 44, 50, 53, 51, 44, 50, 48, 49, 44, 49, 55, 52, 44,
                57, 48, 44, 53, 54, 44, 53, 52, 44, 53, 44, 50, 49, 53, 44, 50, 49, 53, 44, 56, 54,
                44, 50, 51, 54, 44, 50, 51, 50, 44, 54, 52, 44, 51, 56, 44, 49, 53, 52, 44, 52, 51,
                44, 51, 49, 44, 56, 57, 44, 49, 51, 56, 44, 56, 48, 44, 49, 53, 50, 44, 49, 50, 52,
                44, 50, 49, 54, 44, 49, 55, 55, 44, 49, 55, 54, 44, 52, 53, 44, 50, 48, 55, 44, 55,
                50, 44, 50, 50, 54, 44, 49, 55, 55, 44, 49, 49, 54, 44, 50, 48, 55, 44, 49, 50, 50,
                44, 50, 52, 57, 44, 56, 48, 44, 53, 52, 44, 50, 50, 57, 44, 49, 48, 54, 44, 49, 49,
                54, 44, 49, 49, 48, 44, 50, 48, 44, 49, 52, 49, 44, 57, 48, 44, 57, 56, 44, 49, 56,
                53, 44, 50, 49, 56, 44, 49, 48, 49, 44, 50, 50, 56, 44, 49, 53, 48, 44, 49, 56, 57,
                44, 50, 50, 44, 49, 56, 51, 44, 50, 53, 52, 44, 53, 49, 44, 50, 48, 44, 49, 55, 56,
                44, 55, 49, 44, 49, 50, 52, 44, 50, 51, 56, 44, 49, 52, 53, 44, 54, 56, 44, 49, 49,
                51, 44, 56, 49, 44, 50, 51, 44, 50, 51, 56, 44, 50, 52, 54, 44, 49, 48, 49, 44, 52,
                50, 44, 49, 50, 44, 49, 52, 55, 44, 50, 51, 50, 44, 50, 48, 48, 44, 49, 51, 49, 44,
                50, 48, 53, 44, 52, 55, 44, 50, 50, 53, 44, 53, 57, 44, 49, 51, 49, 44, 55, 53, 44,
                51, 55, 44, 50, 51, 55, 44, 49, 56, 48, 44, 53, 44, 49, 48, 49, 44, 54, 57, 44, 49,
                48, 56, 44, 50, 53, 51, 44, 53, 56, 44, 49, 48, 49, 44, 49, 49, 54, 44, 49, 44, 50,
                51, 56, 44, 49, 54, 52, 44, 49, 49, 52, 44, 49, 54, 53, 44, 49, 51, 44, 50, 49, 51,
                44, 49, 55, 50, 44, 49, 55, 53, 44, 49, 50, 51, 44, 54, 55, 44, 49, 56, 51, 44, 50,
                49, 51, 44, 49, 51, 54, 44, 50, 48, 50, 44, 55, 48, 44, 49, 55, 56, 44, 49, 48, 52,
                44, 49, 53, 49, 44, 50, 49, 54, 44, 49, 57, 55, 44, 53, 50, 44, 50, 50, 48, 44, 49,
                50, 49, 44, 49, 52, 54, 44, 50, 50, 51, 44, 49, 55, 53, 44, 54, 50, 44, 50, 53, 50,
                44, 49, 57, 51, 44, 55, 53, 44, 52, 44, 50, 50, 53, 44, 55, 48, 44, 49, 48, 50, 44,
                50, 51, 57, 44, 52, 51, 44, 49, 57, 51, 44, 49, 49, 49, 44, 49, 50, 50, 44, 49, 53,
                44, 49, 49, 48, 44, 49, 50, 48, 44, 57, 52, 44, 49, 49, 51, 44, 51, 52, 44, 50, 48,
                57, 44, 50, 52, 56, 44, 50, 51, 51, 44, 49, 51, 52, 44, 49, 56, 52, 44, 49, 57, 53,
                44, 55, 53, 44, 49, 48, 56, 44, 49, 48, 57, 44, 56, 52, 44, 50, 50, 50, 44, 50, 51,
                49, 44, 49, 49, 51, 44, 49, 51, 48, 44, 50, 52, 51, 44, 49, 48, 56, 44, 52, 51, 44,
                50, 48, 55, 44, 49, 56, 52, 44, 50, 49, 53, 44, 50, 52, 51, 44, 50, 48, 51, 44, 50,
                44, 56, 44, 50, 48, 54, 44, 49, 56, 49, 44, 57, 56, 44, 50, 53, 48, 44, 56, 56, 44,
                50, 51, 56, 44, 49, 48, 53, 44, 50, 53, 51, 44, 50, 52, 57, 44, 49, 49, 48, 44, 49,
                53, 50, 44, 49, 55, 56, 44, 49, 54, 44, 53, 54, 44, 49, 51, 56, 44, 55, 55, 44, 49,
                50, 55, 44, 49, 56, 52, 44, 49, 57, 48, 44, 50, 50, 52, 44, 55, 56, 44, 55, 51, 44,
                50, 49, 51, 44, 49, 50, 55, 44, 50, 53, 44, 50, 51, 50, 44, 52, 52, 44, 57, 51, 44,
                49, 53, 57, 44, 54, 52, 44, 53, 48, 44, 49, 57, 48, 44, 49, 50, 52, 44, 50, 49, 44,
                49, 51, 50, 44, 54, 53, 44, 49, 52, 57, 44, 50, 53, 44, 53, 52, 44, 50, 51, 44, 49,
                52, 44, 50, 49, 48, 44, 50, 49, 53, 44, 56, 51, 44, 49, 50, 52, 44, 52, 51, 44, 49,
                49, 56, 44, 50, 49, 56, 44, 49, 55, 52, 44, 49, 50, 55, 44, 49, 50, 51, 44, 49, 52,
                52, 44, 54, 51, 44, 50, 48, 52, 44, 49, 52, 51, 44, 50, 52, 56, 44, 52, 57, 44, 51,
                53, 44, 48, 44, 50, 53, 50, 44, 50, 48, 53, 44, 55, 52, 44, 49, 50, 56, 44, 49, 52,
                57, 44, 52, 54, 44, 57, 57, 44, 49, 48, 48, 44, 50, 51, 57, 44, 52, 54, 44, 49, 54,
                51, 44, 49, 56, 51, 44, 49, 54, 55, 44, 49, 53, 57, 44, 51, 48, 44, 49, 53, 52, 44,
                56, 53, 44, 54, 55, 44, 54, 49, 44, 50, 53, 44, 50, 52, 48, 44, 49, 55, 48, 44, 50,
                50, 52, 44, 51, 54, 44, 50, 48, 50, 44, 50, 51, 48, 44, 54, 56, 44, 57, 50, 44, 51,
                50, 44, 49, 48, 51, 44, 50, 48, 49, 44, 50, 50, 55, 44, 50, 50, 53, 44, 49, 51, 57,
                44, 49, 56, 52, 44, 57, 52, 44, 50, 53, 51, 44, 49, 53, 51, 44, 49, 48, 48, 44, 49,
                56, 48, 44, 49, 50, 50, 44, 55, 50, 44, 49, 49, 50, 44, 49, 50, 54, 44, 50, 49, 49,
                44, 50, 50, 52, 44, 49, 55, 55, 44, 50, 50, 53, 44, 49, 51, 56, 44, 49, 49, 52, 44,
                50, 49, 54, 44, 57, 55, 44, 49, 51, 55, 44, 55, 53, 44, 54, 56, 44, 49, 51, 52, 44,
                55, 53, 44, 50, 52, 49, 44, 50, 51, 44, 49, 57, 51, 44, 51, 49, 44, 56, 51, 44, 50,
                48, 57, 44, 50, 50, 57, 44, 53, 51, 44, 49, 53, 52, 44, 54, 52, 44, 49, 49, 51, 44,
                50, 51, 51, 44, 50, 53, 52, 44, 49, 54, 50, 44, 50, 51, 44, 54, 44, 50, 51, 44, 49,
                48, 50, 44, 49, 48, 48, 44, 50, 48, 54, 44, 49, 51, 50, 44, 50, 49, 55, 44, 50, 49,
                57, 44, 50, 48, 50, 44, 49, 51, 55, 44, 50, 49, 48, 44, 50, 44, 49, 49, 52, 44, 56,
                44, 49, 54, 44, 49, 57, 51, 44, 49, 50, 52, 44, 57, 52, 44, 49, 52, 51, 44, 50, 51,
                54, 44, 49, 54, 54, 44, 49, 49, 54, 44, 52, 53, 44, 55, 52, 44, 49, 48, 51, 44, 51,
                44, 49, 52, 53, 44, 49, 48, 53, 44, 57, 55, 44, 49, 52, 57, 44, 50, 48, 53, 44, 49,
                53, 55, 44, 50, 51, 49, 44, 50, 49, 48, 44, 56, 57, 44, 50, 49, 54, 44, 57, 48, 44,
                49, 48, 44, 55, 49, 44, 49, 50, 52, 44, 49, 53, 54, 44, 50, 48, 53, 44, 50, 49, 57,
                44, 57, 57, 44, 50, 48, 50, 44, 55, 50, 44, 49, 57, 55, 44, 51, 49, 44, 49, 52, 55,
                44, 50, 52, 51, 44, 49, 54, 57, 44, 50, 49, 54, 44, 55, 55, 44, 50, 56, 44, 50, 44,
                49, 56, 50, 44, 54, 56, 44, 49, 57, 48, 44, 57, 53, 44, 50, 48, 49, 44, 49, 56, 44,
                49, 56, 44, 49, 53, 48, 44, 50, 53, 49, 44, 50, 53, 51, 44, 50, 50, 49, 44, 50, 49,
                44, 49, 54, 50, 44, 49, 48, 44, 50, 49, 52, 44, 49, 50, 50, 44, 50, 51, 48, 44, 53,
                54, 44, 49, 55, 54, 44, 50, 56, 44, 49, 51, 53, 44, 55, 52, 44, 54, 57, 44, 49, 53,
                50, 44, 51, 48, 44, 54, 57, 44, 50, 48, 56, 44, 50, 48, 49, 44, 49, 56, 50, 44, 49,
                53, 57, 44, 49, 48, 50, 44, 49, 49, 56, 44, 54, 57, 44, 49, 55, 44, 49, 49, 50, 44,
                50, 54, 44, 50, 52, 48, 44, 49, 55, 53, 44, 50, 51, 50, 44, 49, 52, 54, 44, 50, 52,
                56, 44, 49, 48, 51, 44, 49, 48, 50, 44, 56, 48, 44, 50, 51, 56, 44, 50, 54, 44, 50,
                52, 54, 44, 50, 50, 44, 49, 57, 44, 54, 44, 49, 56, 55, 44, 49, 56, 53, 44, 49, 51,
                52, 44, 49, 51, 44, 55, 51, 44, 50, 51, 56, 44, 53, 48, 44, 52, 56, 44, 49, 57, 57,
                44, 49, 48, 48, 44, 50, 52, 52, 44, 49, 57, 50, 44, 50, 52, 57, 44, 49, 52, 49, 44,
                49, 51, 53, 44, 49, 49, 53, 44, 50, 53, 49, 44, 55, 51, 44, 49, 52, 55, 44, 49, 56,
                56, 44, 48, 44, 50, 50, 52, 44, 57, 44, 57, 56, 44, 52, 49, 44, 49, 57, 44, 57, 50,
                44, 49, 56, 57, 44, 49, 50, 53, 44, 56, 52, 44, 50, 52, 54, 44, 49, 49, 49, 44, 49,
                50, 56, 44, 50, 50, 44, 49, 54, 49, 44, 49, 57, 57, 44, 49, 54, 57, 44, 56, 52, 44,
                49, 54, 44, 49, 51, 51, 44, 54, 56, 44, 49, 52, 57, 44, 50, 53, 52, 44, 50, 53, 48,
                44, 57, 44, 49, 50, 56, 44, 49, 57, 53, 44, 49, 52, 56, 44, 50, 49, 52, 44, 49, 50,
                48, 44, 50, 50, 54, 44, 48, 44, 50, 53, 50, 44, 49, 54, 48, 44, 55, 55, 44, 49, 54,
                57, 44, 54, 57, 44, 49, 57, 44, 50, 48, 48, 44, 50, 50, 56, 44, 49, 51, 56, 44, 55,
                57, 44, 49, 49, 44, 49, 53, 49, 44, 56, 49, 44, 49, 53, 56, 44, 49, 55, 55, 44, 49,
                56, 44, 50, 52, 51, 44, 49, 56, 53, 44, 49, 50, 52, 44, 50, 50, 53, 44, 51, 50, 44,
                49, 52, 48, 44, 54, 57, 44, 50, 53, 52, 44, 54, 54, 44, 50, 50, 50, 44, 50, 50, 55,
                44, 50, 49, 50, 44, 51, 51, 44, 49, 50, 53, 44, 57, 55, 44, 55, 56, 44, 49, 54, 44,
                53, 48, 44, 53, 52, 44, 49, 51, 44, 52, 53, 44, 49, 53, 53, 44, 49, 54, 55, 44, 50,
                51, 55, 44, 57, 51, 44, 50, 51, 48, 44, 50, 52, 48, 44, 50, 49, 48, 44, 49, 57, 49,
                44, 49, 56, 54, 44, 49, 48, 54, 44, 50, 48, 51, 44, 52, 52, 44, 50, 51, 52, 44, 49,
                52, 53, 44, 50, 51, 48, 44, 53, 50, 44, 49, 50, 44, 48, 44, 49, 51, 51, 44, 49, 52,
                44, 50, 50, 54, 44, 49, 55, 56, 44, 50, 51, 50, 44, 54, 48, 44, 50, 50, 49, 44, 50,
                50, 51, 44, 57, 53, 44, 49, 55, 52, 44, 49, 57, 53, 44, 54, 56, 44, 49, 49, 54, 44,
                49, 48, 50, 44, 57, 52, 44, 57, 51, 44, 49, 51, 57, 44, 50, 52, 57, 44, 49, 51, 53,
                44, 49, 51, 50, 44, 50, 49, 54, 44, 49, 54, 57, 44, 49, 54, 50, 44, 54, 54, 44, 55,
                52, 44, 50, 52, 52, 44, 50, 53, 53, 44, 52, 53, 44, 49, 52, 49, 44, 49, 51, 44, 50,
                48, 56, 44, 57, 44, 50, 51, 57, 44, 49, 50, 52, 44, 49, 52, 57, 44, 52, 56, 44, 50,
                52, 48, 44, 57, 56, 44, 55, 56, 44, 49, 50, 51, 44, 56, 44, 49, 57, 52, 44, 51, 50,
                44, 57, 48, 44, 49, 56, 56, 44, 57, 52, 44, 53, 57, 44, 49, 54, 56, 44, 49, 49, 56,
                44, 49, 56, 52, 44, 49, 54, 54, 44, 49, 56, 48, 44, 51, 48, 44, 49, 49, 48, 44, 53,
                55, 44, 50, 50, 52, 44, 56, 54, 44, 50, 51, 51, 44, 49, 50, 53, 44, 49, 52, 49, 44,
                48, 44, 52, 44, 52, 54, 44, 49, 51, 44, 50, 52, 55, 44, 49, 55, 51, 44, 51, 54, 44,
                49, 48, 44, 57, 57, 44, 56, 55, 44, 49, 57, 54, 44, 53, 52, 44, 49, 56, 48, 44, 49,
                51, 57, 44, 49, 54, 55, 44, 49, 51, 44, 50, 51, 57, 44, 50, 50, 50, 44, 49, 51, 50,
                44, 49, 52, 51, 44, 49, 52, 51, 44, 56, 49, 44, 51, 52, 44, 49, 48, 51, 44, 49, 56,
                53, 44, 53, 50, 44, 49, 53, 51, 44, 50, 53, 52, 44, 52, 52, 44, 49, 53, 55, 44, 53,
                53, 44, 49, 56, 56, 44, 49, 48, 48, 44, 49, 57, 50, 44, 51, 50, 44, 49, 53, 54, 44,
                49, 54, 57, 44, 51, 53, 44, 50, 54, 44, 50, 49, 55, 44, 50, 48, 57, 44, 49, 53, 53,
                44, 49, 48, 54, 44, 52, 48, 44, 53, 48, 44, 52, 57, 44, 49, 53, 53, 44, 50, 52, 51,
                44, 50, 50, 48, 44, 57, 57, 44, 49, 50, 51, 44, 49, 55, 50, 44, 49, 54, 50, 44, 50,
                49, 52, 44, 50, 52, 54, 44, 50, 50, 52, 44, 49, 50, 54, 44, 50, 52, 48, 44, 50, 53,
                50, 44, 50, 51, 48, 44, 49, 57, 48, 44, 50, 52, 53, 44, 49, 55, 52, 44, 54, 56, 44,
                55, 44, 54, 56, 44, 54, 44, 50, 52, 55, 44, 49, 56, 51, 44, 49, 54, 53, 44, 49, 52,
                53, 44, 55, 51, 44, 50, 48, 57, 44, 50, 49, 44, 49, 54, 49, 44, 50, 50, 44, 49, 56,
                55, 44, 50, 53, 50, 44, 49, 48, 55, 44, 49, 55, 48, 44, 49, 51, 51, 44, 53, 51, 44,
                49, 52, 48, 44, 50, 54, 44, 49, 57, 52, 44, 55, 54, 44, 55, 56, 44, 55, 48, 44, 56,
                50, 44, 53, 49, 44, 49, 57, 49, 44, 56, 44, 49, 51, 49, 44, 50, 51, 54, 44, 49, 55,
                52, 44, 52, 52, 44, 49, 57, 54, 44, 55, 56, 44, 50, 51, 50, 44, 53, 54, 44, 49, 56,
                55, 44, 53, 50, 44, 52, 53, 44, 55, 54, 44, 50, 51, 57, 44, 49, 55, 51, 44, 49, 48,
                50, 44, 49, 52, 57, 44, 49, 49, 50, 44, 49, 57, 50, 44, 55, 44, 49, 56, 57, 44, 50,
                53, 53, 44, 52, 50, 44, 50, 51, 55, 44, 49, 55, 48, 44, 50, 51, 55, 44, 50, 52, 50,
                44, 54, 48, 44, 49, 49, 44, 50, 52, 54, 44, 50, 52, 50, 44, 56, 55, 44, 49, 50, 49,
                44, 49, 55, 44, 49, 56, 51, 44, 56, 56, 44, 49, 51, 52, 44, 49, 54, 55, 44, 49, 49,
                48, 44, 51, 56, 44, 50, 55, 44, 49, 50, 52, 44, 52, 44, 49, 49, 53, 44, 55, 56, 44,
                49, 49, 44, 49, 56, 55, 44, 49, 53, 50, 44, 50, 44, 53, 48, 44, 49, 53, 54, 44, 52,
                55, 44, 49, 51, 51, 44, 49, 56, 53, 44, 49, 57, 48, 44, 50, 51, 52, 44, 50, 51, 57,
                44, 49, 49, 49, 44, 55, 50, 44, 50, 50, 52, 44, 49, 49, 52, 44, 55, 50, 44, 49, 55,
                44, 49, 56, 49, 44, 53, 54, 44, 50, 48, 51, 44, 49, 44, 49, 53, 55, 44, 52, 50, 44,
                57, 56, 44, 54, 51, 44, 51, 54, 44, 54, 54, 44, 49, 53, 57, 44, 55, 54, 44, 49, 54,
                52, 44, 52, 50, 44, 50, 53, 51, 44, 49, 56, 49, 44, 49, 50, 53, 44, 56, 53, 44, 49,
                55, 56, 44, 51, 48, 44, 50, 52, 56, 44, 57, 44, 49, 52, 48, 44, 50, 51, 55, 44, 49,
                55, 50, 44, 49, 49, 57, 44, 57, 50, 44, 50, 48, 50, 44, 49, 48, 54, 44, 49, 56, 52,
                44, 53, 56, 44, 49, 55, 53, 44, 50, 49, 56, 44, 49, 53, 50, 44, 49, 51, 54, 44, 56,
                44, 56, 56, 44, 54, 50, 44, 49, 52, 48, 44, 49, 56, 50, 44, 49, 57, 48, 44, 49, 52,
                51, 44, 50, 51, 56, 44, 49, 53, 48, 44, 49, 55, 48, 44, 49, 54, 54, 44, 49, 57, 44,
                49, 51, 49, 44, 49, 51, 56, 44, 49, 53, 49, 44, 54, 49, 44, 50, 48, 48, 44, 56, 48,
                44, 49, 50, 57, 44, 49, 50, 52, 44, 49, 56, 51, 44, 49, 51, 50, 44, 49, 56, 51, 44,
                49, 54, 53, 44, 50, 56, 44, 49, 48, 51, 44, 49, 54, 49, 44, 51, 44, 49, 57, 51, 44,
                50, 49, 52, 44, 49, 56, 50, 44, 50, 49, 44, 50, 52, 56, 44, 49, 50, 51, 44, 52, 48,
                44, 49, 48, 53, 44, 49, 52, 48, 44, 49, 52, 56, 44, 56, 50, 44, 50, 52, 54, 44, 50,
                48, 56, 44, 49, 53, 56, 44, 51, 50, 44, 49, 57, 53, 44, 49, 49, 51, 44, 49, 48, 52,
                44, 50, 48, 50, 44, 56, 54, 44, 49, 50, 56, 44, 50, 48, 54, 44, 49, 57, 57, 44, 49,
                50, 54, 44, 52, 54, 44, 50, 49, 44, 54, 51, 44, 49, 52, 56, 44, 49, 57, 56, 44, 49,
                57, 48, 44, 50, 49, 57, 44, 50, 52, 53, 44, 49, 53, 56, 44, 53, 52, 44, 56, 54, 44,
                49, 53, 55, 44, 57, 57, 44, 55, 52, 44, 50, 50, 53, 44, 49, 49, 50, 44, 49, 48, 55,
                44, 52, 48, 44, 56, 50, 44, 53, 50, 44, 49, 56, 52, 44, 50, 48, 48, 44, 57, 53, 44,
                49, 56, 48, 44, 52, 52, 44, 49, 48, 55, 44, 50, 51, 49, 44, 50, 49, 53, 44, 55, 50,
                44, 49, 54, 56, 44, 57, 52, 44, 50, 50, 53, 44, 50, 51, 50, 44, 50, 51, 53, 44, 49,
                53, 51, 44, 49, 53, 52, 44, 50, 51, 51, 44, 50, 56, 44, 50, 57, 44, 50, 52, 53, 44,
                49, 55, 52, 44, 49, 56, 54, 44, 50, 52, 49, 44, 49, 55, 50, 44, 53, 57, 44, 57, 54,
                44, 50, 50, 57, 44, 52, 44, 49, 57, 48, 44, 50, 49, 51, 44, 50, 44, 52, 53, 44, 49,
                51, 51, 44, 52, 57, 44, 49, 57, 51, 44, 49, 49, 52, 44, 52, 53, 44, 49, 48, 55, 44,
                50, 51, 52, 44, 49, 52, 52, 44, 50, 48, 56, 44, 57, 49, 44, 54, 54, 44, 49, 48, 48,
                44, 49, 56, 48, 44, 53, 50, 44, 56, 44, 49, 48, 48, 44, 49, 48, 44, 49, 54, 51, 44,
                50, 51, 49, 44, 49, 54, 54, 44, 55, 56, 44, 55, 48, 44, 50, 51, 51, 44, 50, 48, 49,
                44, 57, 53, 44, 55, 56, 44, 49, 53, 54, 44, 49, 48, 50, 44, 49, 54, 44, 54, 48, 44,
                52, 49, 44, 49, 55, 51, 44, 49, 50, 52, 44, 56, 50, 44, 57, 49, 44, 53, 57, 44, 50,
                52, 49, 44, 49, 56, 50, 44, 52, 44, 49, 52, 55, 44, 49, 52, 57, 44, 57, 49, 44, 50,
                51, 49, 44, 53, 51, 44, 50, 48, 44, 54, 57, 44, 50, 49, 52, 44, 57, 52, 44, 50, 52,
                49, 44, 53, 48, 44, 50, 52, 57, 44, 49, 53, 51, 44, 56, 57, 44, 49, 57, 49, 44, 49,
                49, 51, 44, 49, 51, 49, 44, 49, 56, 53, 44, 50, 50, 44, 50, 53, 51, 44, 50, 53, 44,
                49, 51, 53, 44, 51, 53, 44, 50, 53, 52, 44, 55, 50, 44, 49, 51, 49, 44, 49, 50, 51,
                44, 51, 54, 44, 49, 52, 49, 44, 50, 52, 44, 49, 48, 44, 49, 50, 55, 44, 49, 55, 53,
                44, 52, 51, 44, 50, 50, 54, 44, 49, 52, 49, 44, 49, 57, 50, 44, 49, 57, 54, 44, 49,
                55, 51, 44, 49, 55, 55, 44, 50, 52, 57, 44, 53, 49, 44, 49, 57, 55, 44, 49, 49, 50,
                44, 53, 48, 44, 51, 48, 44, 49, 55, 52, 44, 49, 53, 50, 44, 50, 48, 53, 44, 51, 56,
                44, 49, 54, 54, 44, 49, 48, 53, 44, 49, 56, 51, 44, 48, 44, 50, 53, 51, 44, 50, 44,
                53, 48, 44, 49, 51, 51, 44, 49, 57, 50, 44, 53, 53, 44, 49, 53, 53, 44, 57, 49, 44,
                49, 53, 56, 44, 49, 57, 50, 44, 49, 55, 50, 44, 54, 53, 44, 52, 50, 44, 49, 51, 54,
                44, 50, 52, 49, 44, 52, 56, 44, 50, 48, 48, 44, 50, 49, 48, 44, 50, 53, 44, 49, 54,
                54, 44, 49, 49, 52, 44, 49, 51, 50, 44, 50, 48, 44, 51, 48, 44, 49, 51, 50, 44, 49,
                57, 48, 44, 50, 51, 49, 44, 50, 56, 44, 50, 52, 56, 44, 49, 54, 44, 50, 49, 50, 44,
                49, 55, 44, 57, 48, 44, 49, 53, 55, 44, 49, 55, 56, 44, 50, 54, 44, 50, 52, 55, 44,
                56, 53, 44, 50, 51, 55, 44, 49, 48, 49, 44, 50, 50, 53, 44, 49, 49, 51, 44, 49, 50,
                57, 44, 51, 49, 44, 51, 57, 44, 49, 51, 54, 44, 50, 48, 51, 44, 53, 52, 44, 49, 55,
                48, 44, 57, 50, 44, 52, 50, 44, 50, 51, 44, 50, 48, 44, 57, 54, 44, 50, 53, 49, 44,
                49, 52, 44, 49, 55, 53, 44, 50, 51, 48, 44, 50, 50, 56, 44, 49, 53, 44, 50, 51, 57,
                44, 56, 56, 44, 50, 52, 49, 44, 49, 52, 44, 55, 44, 49, 52, 55, 44, 49, 50, 56, 44,
                51, 56, 44, 50, 48, 51, 44, 50, 48, 49, 44, 57, 44, 53, 53, 44, 50, 50, 54, 44, 53,
                49, 44, 49, 52, 49, 44, 49, 50, 44, 49, 55, 53, 44, 50, 51, 44, 49, 52, 53, 44, 49,
                55, 49, 44, 57, 57, 44, 49, 55, 49, 44, 49, 56, 55, 44, 50, 49, 52, 44, 49, 53, 53,
                44, 50, 48, 56, 44, 50, 52, 56, 44, 50, 49, 53, 44, 51, 54, 44, 49, 57, 55, 44, 57,
                52, 44, 57, 44, 50, 49, 50, 44, 49, 49, 54, 44, 53, 50, 44, 49, 50, 49, 44, 49, 52,
                49, 44, 49, 54, 53, 44, 50, 52, 49, 44, 54, 50, 44, 51, 53, 44, 53, 51, 44, 49, 48,
                44, 49, 48, 52, 44, 55, 52, 44, 55, 55, 44, 52, 51, 44, 49, 53, 51, 44, 54, 52, 44,
                50, 52, 50, 44, 50, 49, 57, 44, 49, 55, 44, 51, 48, 44, 49, 57, 50, 44, 57, 54, 44,
                50, 51, 44, 56, 55, 44, 50, 50, 52, 44, 49, 48, 53, 44, 49, 48, 56, 44, 49, 53, 44,
                49, 55, 49, 44, 52, 55, 44, 49, 57, 53, 44, 55, 44, 54, 52, 44, 50, 53, 51, 44, 49,
                48, 54, 44, 57, 57, 44, 49, 50, 51, 44, 50, 51, 51, 44, 55, 50, 44, 49, 55, 56, 44,
                53, 50, 44, 49, 52, 54, 44, 50, 48, 44, 49, 54, 57, 44, 49, 54, 51, 44, 49, 56, 53,
                44, 50, 48, 44, 49, 55, 49, 44, 50, 52, 49, 44, 51, 52, 44, 49, 50, 55, 44, 50, 51,
                49, 44, 49, 56, 57, 44, 49, 53, 49, 44, 49, 57, 50, 44, 49, 53, 56, 44, 49, 50, 55,
                44, 49, 52, 48, 44, 50, 50, 55, 44, 53, 51, 44, 49, 57, 55, 44, 50, 49, 49, 44, 53,
                51, 44, 49, 44, 49, 54, 48, 44, 54, 49, 44, 49, 52, 44, 50, 49, 49, 44, 51, 44, 52,
                57, 44, 49, 55, 49, 44, 57, 48, 44, 50, 52, 56, 44, 50, 52, 53, 44, 49, 51, 51, 44,
                55, 53, 44, 49, 48, 53, 44, 49, 52, 49, 44, 55, 54, 44, 52, 53, 44, 52, 50, 44, 50,
                51, 52, 44, 53, 48, 44, 56, 51, 44, 50, 53, 48, 44, 49, 53, 53, 44, 50, 54, 44, 49,
                52, 52, 44, 50, 50, 52, 44, 49, 56, 55, 44, 50, 52, 44, 49, 49, 50, 44, 49, 50, 49,
                44, 50, 53, 48, 44, 57, 54, 44, 51, 54, 44, 56, 52, 44, 49, 53, 54, 44, 50, 50, 44,
                50, 52, 51, 44, 54, 53, 44, 52, 56, 44, 56, 54, 44, 49, 51, 50, 44, 53, 57, 44, 50,
                53, 50, 44, 49, 54, 56, 44, 50, 49, 56, 44, 49, 55, 44, 49, 55, 49, 44, 52, 51, 44,
                52, 54, 44, 49, 50, 56, 44, 49, 52, 55, 44, 53, 57, 44, 49, 55, 49, 44, 50, 48, 48,
                44, 50, 53, 44, 53, 55, 44, 49, 50, 49, 44, 50, 49, 52, 44, 49, 54, 48, 44, 50, 50,
                53, 44, 57, 56, 44, 54, 53, 44, 49, 56, 55, 44, 49, 51, 55, 44, 54, 51, 44, 49, 51,
                52, 44, 50, 49, 53, 44, 49, 53, 50, 44, 52, 48, 44, 50, 53, 44, 49, 49, 52, 44, 49,
                48, 53, 44, 49, 56, 49, 44, 50, 51, 57, 44, 54, 44, 49, 50, 44, 49, 54, 52, 44, 50,
                52, 49, 44, 50, 51, 44, 49, 52, 44, 50, 48, 44, 50, 51, 54, 44, 49, 51, 49, 44, 51,
                50, 44, 50, 50, 56, 44, 50, 49, 49, 44, 55, 55, 44, 49, 51, 53, 44, 55, 51, 44, 49,
                55, 52, 44, 49, 56, 54, 44, 55, 54, 44, 49, 50, 54, 44, 52, 44, 56, 54, 44, 50, 48,
                57, 44, 54, 48, 44, 57, 56, 44, 50, 48, 51, 44, 49, 56, 53, 44, 49, 55, 54, 44, 50,
                49, 49, 44, 51, 56, 44, 49, 51, 52, 44, 50, 51, 54, 44, 49, 48, 49, 44, 49, 53, 55,
                44, 55, 54, 44, 55, 53, 44, 52, 50, 44, 49, 54, 48, 44, 57, 54, 44, 49, 51, 44, 49,
                54, 51, 44, 49, 50, 53, 44, 49, 49, 50, 44, 49, 56, 44, 52, 44, 53, 51, 44, 50, 48,
                51, 44, 49, 49, 56, 44, 49, 50, 49, 44, 56, 55, 44, 50, 51, 44, 49, 57, 51, 44, 49,
                52, 55, 44, 49, 49, 51, 44, 50, 52, 51, 44, 49, 55, 49, 44, 50, 49, 49, 44, 49, 53,
                48, 44, 49, 49, 51, 44, 57, 55, 44, 49, 51, 48, 44, 56, 55, 44, 57, 57, 44, 55, 44,
                53, 54, 44, 50, 51, 52, 44, 57, 52, 44, 49, 50, 49, 44, 50, 51, 48, 44, 50, 52, 48,
                44, 50, 52, 52, 44, 49, 54, 56, 44, 49, 51, 52, 44, 49, 55, 52, 44, 49, 54, 49, 93,
                44, 34, 116, 111, 107, 101, 110, 95, 97, 115, 115, 105, 103, 110, 109, 101, 110,
                116, 34, 58, 123, 34, 116, 105, 101, 114, 34, 58, 34, 68, 97, 121, 49, 34, 44, 34,
                116, 105, 109, 101, 95, 115, 108, 111, 116, 34, 58, 34, 50, 48, 50, 50, 45, 48, 54,
                45, 50, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 34, 44, 34, 103, 101, 110, 101,
                114, 97, 116, 111, 114, 34, 58, 123, 34, 110, 34, 58, 91, 50, 51, 49, 51, 48, 54,
                53, 55, 55, 44, 49, 49, 53, 57, 51, 54, 52, 48, 44, 55, 53, 52, 54, 54, 53, 54, 56,
                52, 44, 52, 51, 54, 51, 50, 55, 50, 53, 57, 44, 50, 49, 48, 51, 56, 56, 50, 54, 54,
                56, 44, 50, 53, 49, 51, 55, 48, 48, 48, 52, 44, 49, 51, 48, 54, 55, 54, 54, 48, 51,
                50, 44, 52, 48, 55, 56, 53, 52, 48, 49, 52, 51, 44, 50, 50, 55, 50, 53, 49, 56, 51,
                53, 54, 44, 50, 51, 57, 48, 51, 48, 49, 48, 48, 49, 44, 50, 56, 50, 55, 50, 53, 49,
                49, 57, 51, 44, 51, 55, 56, 53, 54, 54, 54, 50, 51, 52, 44, 54, 53, 48, 54, 53, 49,
                54, 53, 48, 44, 49, 57, 57, 56, 56, 50, 52, 49, 49, 50, 44, 52, 48, 48, 56, 52, 49,
                48, 50, 49, 49, 44, 51, 55, 55, 49, 51, 56, 52, 52, 52, 49, 44, 52, 49, 52, 50, 48,
                57, 50, 49, 52, 55, 44, 51, 57, 55, 54, 50, 56, 51, 48, 55, 51, 44, 55, 52, 57, 55,
                50, 50, 50, 54, 48, 44, 49, 56, 52, 51, 56, 56, 55, 57, 51, 53, 44, 50, 55, 56, 48,
                57, 53, 55, 49, 57, 57, 44, 51, 51, 55, 57, 48, 49, 49, 55, 55, 57, 44, 50, 52, 50,
                48, 51, 51, 53, 49, 54, 56, 44, 49, 53, 54, 55, 55, 51, 49, 55, 50, 50, 44, 51, 50,
                50, 57, 48, 50, 55, 55, 51, 53, 44, 49, 48, 57, 50, 50, 50, 55, 57, 50, 48, 44, 50,
                54, 51, 49, 55, 51, 49, 55, 55, 56, 44, 53, 50, 48, 56, 50, 57, 50, 53, 56, 44, 51,
                49, 48, 55, 55, 57, 57, 50, 50, 44, 50, 50, 52, 57, 57, 49, 48, 56, 54, 52, 44, 53,
                54, 53, 56, 48, 50, 57, 55, 54, 44, 49, 51, 52, 57, 52, 51, 52, 51, 48, 55, 44, 50,
                48, 55, 54, 51, 51, 48, 49, 48, 49, 44, 49, 56, 56, 48, 49, 57, 56, 51, 57, 52, 44,
                52, 50, 48, 56, 50, 49, 51, 51, 54, 56, 44, 51, 53, 48, 48, 49, 49, 54, 55, 55, 50,
                44, 51, 53, 50, 52, 48, 49, 49, 50, 55, 44, 49, 54, 57, 49, 50, 52, 54, 52, 56, 54,
                44, 52, 49, 52, 48, 56, 49, 48, 57, 50, 44, 51, 53, 56, 56, 54, 54, 54, 52, 51, 48,
                44, 51, 50, 49, 57, 56, 50, 55, 51, 50, 54, 44, 51, 50, 49, 51, 53, 50, 54, 52, 54,
                54, 44, 49, 52, 55, 50, 49, 54, 55, 56, 55, 44, 51, 48, 56, 56, 49, 56, 51, 48, 51,
                57, 44, 51, 53, 52, 53, 56, 56, 54, 54, 51, 51, 44, 57, 57, 56, 51, 50, 56, 57, 48,
                49, 44, 56, 57, 51, 51, 52, 53, 56, 50, 53, 44, 50, 53, 49, 50, 52, 54, 53, 51, 57,
                44, 55, 55, 52, 55, 55, 54, 49, 52, 56, 44, 49, 52, 57, 53, 48, 52, 56, 54, 55, 48,
                44, 51, 49, 51, 50, 57, 50, 50, 48, 55, 52, 44, 50, 56, 50, 52, 57, 49, 53, 50, 55,
                50, 44, 53, 53, 50, 52, 48, 56, 51, 56, 52, 44, 51, 57, 53, 53, 57, 48, 50, 52, 53,
                56, 44, 51, 55, 51, 55, 49, 52, 50, 51, 52, 49, 44, 52, 48, 48, 48, 53, 52, 51, 55,
                49, 49, 44, 50, 52, 57, 56, 51, 55, 55, 51, 53, 57, 44, 49, 54, 54, 53, 50, 49, 52,
                57, 54, 57, 44, 53, 49, 48, 53, 57, 52, 55, 55, 53, 44, 52, 57, 55, 55, 54, 48, 51,
                52, 51, 44, 50, 52, 52, 51, 48, 55, 57, 56, 49, 51, 44, 56, 55, 57, 53, 49, 55, 56,
                56, 51, 44, 49, 50, 54, 52, 55, 51, 56, 57, 53, 55, 44, 51, 53, 57, 51, 51, 56, 50,
                52, 51, 53, 44, 54, 51, 51, 55, 51, 49, 51, 53, 51, 44, 57, 51, 48, 53, 54, 55, 49,
                49, 44, 51, 48, 50, 48, 49, 49, 53, 52, 55, 55, 44, 49, 49, 51, 51, 55, 53, 50, 49,
                50, 44, 51, 49, 52, 55, 51, 48, 51, 48, 55, 51, 44, 51, 54, 53, 52, 56, 49, 49, 55,
                57, 51, 44, 50, 55, 55, 55, 52, 52, 53, 56, 50, 54, 44, 49, 51, 56, 54, 50, 51, 56,
                52, 51, 52, 44, 57, 52, 53, 57, 52, 51, 56, 56, 52, 44, 52, 48, 53, 48, 54, 55, 57,
                56, 55, 50, 44, 54, 57, 56, 52, 52, 56, 49, 50, 57, 44, 51, 52, 50, 52, 56, 57, 48,
                56, 57, 57, 44, 51, 52, 54, 51, 54, 54, 49, 48, 55, 53, 44, 49, 53, 51, 53, 56, 52,
                54, 52, 50, 53, 44, 49, 56, 57, 50, 57, 54, 52, 57, 49, 56, 44, 49, 56, 50, 49, 50,
                53, 53, 52, 56, 54, 44, 52, 49, 49, 55, 57, 53, 49, 48, 48, 55, 44, 51, 52, 48, 50,
                54, 57, 53, 53, 50, 44, 51, 50, 49, 50, 57, 50, 55, 52, 54, 55, 44, 50, 50, 56, 53,
                57, 51, 48, 52, 50, 50, 44, 49, 48, 54, 52, 55, 48, 57, 50, 56, 54, 44, 49, 56, 48,
                52, 54, 52, 49, 48, 49, 56, 44, 53, 53, 52, 51, 51, 49, 51, 51, 52, 44, 57, 49, 49,
                57, 55, 53, 54, 54, 55, 44, 49, 57, 55, 55, 52, 52, 50, 54, 50, 50, 44, 52, 49, 52,
                50, 51, 57, 52, 57, 52, 48, 44, 49, 55, 54, 54, 51, 52, 56, 54, 54, 44, 50, 56, 48,
                55, 49, 56, 53, 57, 53, 55, 44, 49, 50, 49, 53, 54, 52, 54, 52, 50, 48, 44, 50, 48,
                49, 48, 49, 51, 52, 56, 57, 49, 44, 49, 57, 49, 52, 51, 49, 53, 54, 48, 49, 44, 49,
                55, 57, 53, 56, 55, 51, 50, 55, 48, 44, 51, 52, 51, 57, 56, 55, 55, 54, 51, 52, 44,
                52, 49, 56, 57, 49, 57, 48, 57, 50, 52, 44, 50, 50, 55, 49, 52, 57, 50, 49, 50, 50,
                44, 54, 50, 55, 51, 48, 50, 49, 55, 49, 44, 49, 50, 53, 52, 50, 54, 55, 49, 51, 49,
                44, 51, 49, 55, 48, 51, 52, 52, 55, 48, 51, 44, 49, 54, 53, 56, 55, 56, 54, 48, 51,
                56, 44, 51, 49, 56, 53, 53, 53, 49, 52, 57, 57, 44, 51, 49, 51, 52, 50, 48, 51, 49,
                56, 44, 51, 56, 49, 55, 50, 49, 55, 51, 49, 52, 44, 49, 55, 50, 48, 51, 51, 49, 51,
                57, 48, 44, 51, 55, 50, 56, 50, 57, 49, 57, 44, 52, 50, 53, 49, 54, 49, 56, 56, 53,
                48, 44, 51, 52, 53, 56, 49, 51, 52, 57, 54, 52, 44, 49, 55, 49, 51, 49, 57, 48, 54,
                55, 53, 44, 50, 50, 51, 52, 52, 52, 55, 50, 53, 53, 44, 51, 53, 55, 56, 53, 54, 57,
                53, 56, 57, 44, 52, 49, 56, 55, 56, 51, 53, 48, 52, 50, 44, 53, 54, 48, 49, 53, 50,
                54, 57, 53, 44, 51, 57, 51, 51, 53, 50, 55, 57, 51, 57, 44, 50, 49, 51, 54, 53, 50,
                48, 55, 52, 52, 44, 50, 51, 48, 54, 48, 57, 52, 54, 57, 49, 44, 50, 50, 52, 49, 54,
                54, 49, 55, 54, 48, 44, 51, 49, 50, 49, 56, 55, 50, 56, 57, 56, 44, 51, 54, 53, 48,
                51, 51, 55, 55, 49, 50, 44, 49, 53, 56, 50, 53, 54, 54, 55, 49, 55, 44, 50, 50, 57,
                55, 51, 49, 53, 49, 57, 48, 44, 50, 53, 50, 55, 50, 51, 48, 54, 57, 44, 51, 54, 49,
                48, 51, 48, 51, 57, 55, 49, 44, 50, 49, 49, 56, 55, 55, 50, 56, 50, 55, 44, 50, 49,
                54, 54, 50, 54, 56, 53, 49, 51, 44, 50, 56, 49, 50, 55, 53, 55, 51, 54, 50, 93, 44,
                34, 101, 34, 58, 91, 54, 53, 53, 51, 55, 93, 125, 44, 34, 115, 105, 103, 110, 97,
                116, 117, 114, 101, 34, 58, 91, 49, 52, 50, 44, 51, 52, 44, 49, 55, 55, 44, 49, 54,
                56, 44, 53, 53, 44, 50, 51, 50, 44, 54, 53, 44, 54, 44, 56, 55, 44, 49, 50, 53, 44,
                57, 51, 44, 50, 48, 49, 44, 49, 49, 55, 44, 49, 49, 44, 49, 48, 49, 44, 49, 55, 49,
                44, 50, 49, 51, 44, 50, 50, 48, 44, 49, 53, 56, 44, 54, 54, 44, 49, 55, 49, 44, 50,
                48, 51, 44, 50, 52, 53, 44, 49, 53, 49, 44, 49, 48, 50, 44, 54, 50, 44, 49, 55, 51,
                44, 50, 52, 49, 44, 49, 56, 53, 44, 50, 49, 56, 44, 55, 52, 44, 50, 52, 53, 44, 49,
                54, 53, 44, 49, 51, 56, 44, 51, 55, 44, 50, 52, 54, 44, 49, 56, 50, 44, 49, 55, 49,
                44, 49, 54, 50, 44, 49, 54, 52, 44, 49, 57, 44, 49, 56, 49, 44, 49, 51, 54, 44, 50,
                50, 50, 44, 54, 57, 44, 57, 44, 49, 57, 55, 44, 50, 49, 53, 44, 53, 56, 44, 50, 56,
                44, 49, 54, 51, 44, 49, 53, 52, 44, 57, 54, 44, 50, 53, 48, 44, 52, 55, 44, 56, 56,
                44, 57, 48, 44, 49, 54, 54, 44, 57, 49, 44, 50, 48, 54, 44, 53, 50, 44, 49, 52, 50,
                44, 53, 51, 44, 49, 54, 51, 44, 52, 55, 44, 49, 52, 56, 44, 55, 53, 44, 50, 55, 44,
                49, 56, 44, 49, 44, 50, 52, 52, 44, 49, 50, 53, 44, 50, 50, 57, 44, 57, 56, 44, 57,
                56, 44, 49, 52, 49, 44, 49, 51, 54, 44, 53, 52, 44, 50, 53, 51, 44, 50, 53, 49, 44,
                49, 49, 44, 50, 53, 48, 44, 50, 50, 56, 44, 50, 48, 48, 44, 50, 48, 53, 44, 49, 54,
                49, 44, 50, 49, 48, 44, 54, 57, 44, 56, 55, 44, 53, 49, 44, 49, 55, 51, 44, 53, 48,
                44, 50, 51, 57, 44, 53, 53, 44, 49, 49, 54, 44, 49, 56, 54, 44, 49, 54, 55, 44, 49,
                49, 51, 44, 50, 53, 52, 44, 50, 48, 54, 44, 49, 48, 53, 44, 52, 51, 44, 49, 54, 49,
                44, 50, 48, 51, 44, 49, 49, 48, 44, 50, 52, 56, 44, 50, 51, 49, 44, 49, 49, 50, 44,
                50, 51, 54, 44, 57, 53, 44, 50, 52, 57, 44, 50, 50, 49, 44, 50, 52, 53, 44, 50, 53,
                51, 44, 51, 53, 44, 50, 50, 52, 44, 51, 57, 44, 55, 54, 44, 55, 51, 44, 49, 55, 57,
                44, 50, 53, 48, 44, 50, 48, 44, 50, 48, 51, 44, 49, 54, 44, 49, 55, 53, 44, 49, 57,
                52, 44, 50, 49, 53, 44, 53, 56, 44, 48, 44, 49, 49, 49, 44, 49, 48, 49, 44, 49, 55,
                44, 53, 53, 44, 57, 56, 44, 50, 50, 55, 44, 49, 54, 53, 44, 49, 56, 55, 44, 49, 57,
                44, 52, 56, 44, 51, 56, 44, 50, 51, 52, 44, 49, 55, 54, 44, 50, 52, 49, 44, 50, 48,
                54, 44, 49, 55, 50, 44, 49, 44, 49, 55, 44, 50, 48, 49, 44, 49, 49, 54, 44, 52, 55,
                44, 50, 52, 48, 44, 49, 56, 54, 44, 53, 55, 44, 50, 53, 50, 44, 53, 51, 44, 49, 49,
                57, 44, 49, 49, 50, 44, 49, 55, 55, 44, 49, 51, 56, 44, 49, 56, 50, 44, 50, 50, 50,
                44, 50, 50, 44, 55, 52, 44, 50, 50, 56, 44, 49, 51, 51, 44, 51, 49, 44, 50, 50, 55,
                44, 49, 51, 54, 44, 50, 48, 54, 44, 49, 54, 44, 55, 53, 44, 50, 53, 44, 49, 57, 56,
                44, 55, 57, 44, 49, 49, 52, 44, 50, 52, 50, 44, 49, 55, 57, 44, 49, 57, 55, 44, 50,
                53, 44, 49, 49, 50, 44, 57, 49, 44, 49, 51, 53, 44, 49, 52, 55, 44, 50, 50, 50, 44,
                56, 49, 44, 49, 53, 54, 44, 53, 51, 44, 49, 52, 55, 44, 49, 57, 53, 44, 50, 49, 56,
                44, 50, 48, 54, 44, 49, 54, 48, 44, 49, 56, 56, 44, 57, 56, 44, 49, 51, 53, 44, 51,
                51, 44, 54, 50, 44, 49, 53, 44, 50, 49, 55, 44, 57, 49, 44, 50, 51, 48, 44, 49, 54,
                50, 44, 51, 54, 44, 50, 51, 44, 49, 51, 50, 44, 50, 48, 54, 44, 50, 51, 52, 44, 49,
                50, 52, 44, 53, 48, 44, 50, 51, 52, 44, 50, 48, 56, 44, 50, 53, 53, 44, 50, 50, 51,
                44, 49, 54, 48, 44, 50, 49, 55, 44, 50, 51, 52, 44, 50, 56, 44, 55, 55, 44, 57, 53,
                44, 50, 49, 55, 44, 49, 55, 52, 44, 54, 55, 44, 52, 52, 44, 50, 50, 49, 44, 49, 51,
                44, 49, 54, 50, 44, 49, 55, 53, 44, 50, 52, 55, 44, 50, 48, 48, 44, 51, 44, 49, 54,
                49, 44, 51, 52, 44, 49, 54, 57, 44, 49, 49, 56, 44, 52, 52, 44, 49, 56, 53, 44, 49,
                55, 51, 44, 49, 53, 52, 44, 49, 51, 55, 44, 49, 52, 52, 44, 49, 49, 52, 44, 55, 52,
                44, 50, 52, 55, 44, 53, 53, 44, 55, 51, 44, 49, 48, 48, 44, 50, 48, 50, 44, 49, 44,
                49, 55, 48, 44, 49, 54, 53, 44, 49, 54, 52, 44, 50, 50, 53, 44, 49, 54, 44, 52, 49,
                44, 49, 55, 56, 44, 49, 52, 53, 44, 49, 53, 52, 44, 50, 50, 48, 44, 49, 48, 56, 44,
                55, 54, 44, 50, 44, 49, 57, 44, 51, 53, 44, 54, 57, 44, 51, 53, 44, 49, 56, 48, 44,
                49, 57, 53, 44, 51, 55, 44, 51, 56, 44, 49, 54, 44, 52, 51, 44, 49, 56, 54, 44, 49,
                55, 55, 44, 49, 57, 44, 49, 49, 51, 44, 49, 50, 57, 44, 51, 52, 44, 50, 50, 55, 44,
                57, 49, 44, 56, 52, 44, 51, 50, 44, 50, 48, 49, 44, 50, 52, 50, 44, 49, 44, 49, 57,
                52, 44, 49, 57, 54, 44, 50, 49, 57, 44, 54, 52, 44, 50, 52, 53, 44, 49, 49, 53, 44,
                50, 56, 44, 49, 50, 49, 44, 54, 57, 44, 49, 56, 54, 44, 50, 50, 54, 44, 49, 50, 49,
                44, 50, 50, 50, 44, 51, 54, 44, 50, 50, 49, 44, 49, 50, 44, 50, 48, 48, 44, 53, 52,
                44, 50, 48, 53, 44, 51, 53, 44, 50, 48, 48, 44, 55, 55, 44, 50, 48, 56, 44, 52, 57,
                44, 50, 50, 44, 49, 55, 49, 44, 49, 52, 54, 44, 49, 55, 54, 44, 49, 54, 54, 44, 49,
                55, 54, 44, 49, 56, 51, 44, 49, 54, 53, 44, 53, 54, 44, 50, 44, 49, 52, 56, 44, 50,
                51, 56, 44, 50, 53, 50, 44, 50, 49, 55, 44, 51, 53, 44, 50, 50, 52, 44, 52, 52, 44,
                49, 50, 55, 44, 50, 49, 52, 44, 50, 51, 50, 44, 55, 50, 44, 53, 57, 44, 54, 54, 44,
                57, 51, 44, 50, 51, 53, 44, 50, 49, 44, 49, 55, 44, 50, 50, 52, 44, 49, 54, 56, 44,
                56, 51, 44, 50, 52, 51, 44, 49, 56, 55, 44, 51, 52, 44, 49, 52, 57, 44, 52, 56, 44,
                52, 49, 44, 50, 49, 51, 44, 49, 57, 53, 44, 57, 52, 44, 49, 53, 50, 44, 50, 51, 53,
                44, 49, 56, 55, 44, 49, 49, 55, 44, 50, 49, 51, 44, 50, 48, 49, 44, 49, 49, 52, 44,
                54, 54, 44, 49, 51, 55, 44, 49, 50, 54, 44, 49, 48, 51, 44, 50, 49, 53, 44, 50, 49,
                55, 44, 49, 57, 50, 44, 55, 54, 44, 49, 56, 44, 49, 55, 48, 44, 56, 48, 44, 50, 51,
                49, 44, 50, 50, 56, 44, 55, 56, 44, 50, 51, 51, 44, 49, 50, 56, 44, 50, 49, 44, 49,
                51, 50, 44, 50, 44, 50, 52, 53, 44, 53, 48, 44, 50, 52, 48, 44, 50, 50, 53, 44, 50,
                51, 57, 44, 50, 48, 55, 44, 50, 51, 48, 44, 50, 49, 56, 44, 55, 53, 44, 49, 48, 44,
                55, 57, 44, 49, 49, 44, 50, 51, 54, 44, 50, 49, 56, 44, 50, 52, 51, 44, 55, 49, 44,
                49, 49, 49, 44, 49, 51, 52, 44, 51, 49, 44, 49, 54, 53, 44, 49, 54, 49, 44, 50, 49,
                53, 44, 57, 57, 44, 52, 48, 44, 49, 53, 57, 44, 55, 48, 44, 55, 57, 44, 55, 48, 44,
                50, 50, 44, 49, 56, 55, 44, 49, 57, 56, 44, 53, 48, 44, 49, 57, 50, 44, 52, 57, 44,
                50, 49, 56, 44, 52, 54, 44, 53, 51, 44, 49, 54, 57, 44, 56, 53, 44, 49, 50, 52, 44,
                53, 44, 49, 49, 52, 44, 53, 54, 44, 50, 48, 53, 44, 50, 52, 55, 44, 49, 55, 53, 44,
                49, 50, 49, 44, 50, 48, 50, 44, 50, 52, 56, 44, 50, 53, 49, 44, 49, 50, 51, 44, 54,
                57, 44, 49, 55, 57, 44, 49, 50, 56, 44, 53, 50, 44, 50, 48, 49, 44, 49, 51, 51, 44,
                50, 48, 53, 44, 49, 52, 48, 44, 49, 52, 49, 44, 50, 49, 56, 44, 52, 49, 44, 51, 44,
                49, 53, 48, 44, 55, 44, 50, 53, 52, 44, 49, 57, 51, 44, 54, 54, 44, 53, 48, 44, 50,
                53, 53, 44, 49, 56, 49, 44, 49, 50, 49, 44, 49, 48, 55, 44, 50, 51, 48, 44, 49, 56,
                49, 44, 56, 57, 44, 49, 54, 54, 44, 49, 50, 52, 44, 54, 51, 44, 52, 56, 44, 49, 52,
                55, 44, 49, 48, 48, 44, 50, 56, 44, 49, 49, 50, 44, 56, 52, 44, 49, 49, 52, 44, 56,
                54, 44, 49, 52, 51, 44, 51, 57, 44, 49, 57, 53, 44, 49, 48, 54, 44, 52, 53, 44, 52,
                53, 44, 55, 51, 44, 56, 44, 49, 51, 51, 44, 51, 53, 44, 50, 53, 44, 56, 50, 44, 50,
                56, 44, 50, 48, 44, 50, 50, 57, 44, 51, 56, 44, 55, 55, 44, 49, 50, 51, 44, 49, 53,
                57, 44, 50, 50, 52, 44, 55, 54, 44, 50, 48, 56, 44, 56, 50, 44, 52, 53, 44, 50, 53,
                52, 44, 54, 57, 44, 49, 48, 56, 44, 57, 54, 44, 49, 55, 44, 50, 48, 51, 44, 57, 49,
                44, 56, 51, 44, 50, 52, 49, 44, 49, 52, 53, 44, 49, 55, 50, 44, 49, 53, 56, 44, 57,
                53, 44, 49, 54, 55, 44, 49, 53, 48, 44, 49, 55, 48, 44, 51, 51, 44, 53, 51, 44, 50,
                49, 54, 44, 52, 50, 44, 48, 44, 49, 55, 54, 44, 54, 49, 44, 56, 44, 52, 51, 44, 49,
                52, 49, 44, 52, 44, 49, 56, 51, 93, 44, 34, 97, 115, 115, 105, 103, 110, 109, 101,
                110, 116, 95, 104, 97, 115, 104, 34, 58, 91, 53, 56, 44, 57, 48, 44, 49, 52, 44,
                55, 50, 44, 54, 57, 44, 56, 49, 44, 57, 56, 44, 49, 52, 49, 44, 50, 49, 56, 44, 49,
                56, 52, 44, 55, 55, 44, 50, 53, 44, 49, 51, 52, 44, 50, 54, 44, 51, 56, 44, 57, 49,
                44, 51, 55, 44, 51, 56, 44, 50, 56, 44, 55, 50, 44, 49, 55, 55, 44, 50, 49, 44, 50,
                53, 53, 44, 49, 56, 50, 44, 56, 48, 44, 49, 51, 50, 44, 50, 52, 54, 44, 55, 48, 44,
                49, 53, 57, 44, 54, 51, 44, 49, 53, 53, 44, 50, 52, 57, 93, 44, 34, 116, 111, 107,
                101, 110, 95, 114, 101, 99, 111, 114, 100, 34, 58, 91, 49, 48, 48, 44, 48, 44, 49,
                52, 44, 49, 44, 53, 44, 54, 50, 44, 49, 48, 56, 44, 55, 48, 44, 49, 53, 48, 44, 49,
                51, 50, 44, 55, 57, 44, 57, 56, 44, 52, 52, 44, 50, 50, 53, 44, 50, 51, 55, 44, 49,
                55, 54, 44, 49, 51, 53, 44, 50, 52, 52, 44, 51, 53, 44, 49, 56, 49, 44, 49, 54, 54,
                44, 57, 57, 44, 56, 53, 44, 49, 50, 56, 44, 55, 55, 44, 49, 52, 51, 44, 49, 50, 55,
                44, 49, 49, 53, 44, 54, 56, 44, 49, 52, 52, 44, 49, 53, 57, 44, 49, 48, 52, 93,
                125, 125, 93, 125, 125,
            ]
            .into(),
        );

        let update_modification = Inbox::update_state(
            params.clone(),
            State::from(state_bytes.clone()),
            vec![update],
        )
        .unwrap();

        let UpdateModification {
            new_state, related, ..
        } = update_modification;

        // TODO: Replace with the expected state
        let state = State::from(vec![]);

        let update = UpdateData::RelatedState {
            related_to: related.get(0).unwrap().contract_instance_id,
            state,
        };
        let update_modification =
            Inbox::update_state(params, State::from(state_bytes), vec![update]).unwrap();

        Ok(())
    }
}
