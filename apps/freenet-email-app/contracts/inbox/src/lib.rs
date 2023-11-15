use std::collections::{HashMap, HashSet};
use std::fmt::Display;

use chrono::{DateTime, Utc};
use freenet_aft_interface::{
    InvalidReason as TokenInvalidReason, Tier, TokenAllocationRecord, TokenAssignment,
    TokenAssignmentHash,
};
use freenet_stdlib::prelude::*;
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

impl Default for InboxSettings {
    fn default() -> Self {
        Self {
            minimum_tier: Tier::Min30,
            private: Default::default(),
        }
    }
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

    pub fn serialize(&self) -> Result<Vec<u8>, ContractError> {
        serde_json::to_vec(self).map_err(|err| ContractError::Deser(format!("{err}")))
    }

    pub fn sign(key: &RsaPrivateKey) -> Signature {
        let signing_key = SigningKey::<Sha256>::new(key.clone());
        signing_key.sign(STATE_UPDATE).into()
    }

    fn verify(&self, params: &InboxParams) -> Result<(), VerificationError> {
        let verifying_key = VerifyingKey::<Sha256>::new(params.pub_key.clone());
        verifying_key
            .verify(
                STATE_UPDATE,
                &rsa::pkcs1v15::Signature::try_from(&*self.inbox_signature).unwrap(),
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
            let Some(records) = allocation_records.get(&message.token_assignment.token_record)
            else {
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
                VerifyingKey::<Sha256>::new(message.token_assignment.generator.clone());
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
        let verifying_key = VerifyingKey::<Sha256>::new(message.token_assignment.generator.clone());
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
                            // freenet_stdlib::log::info(&format!("veryifying key inbox: `{pk}`"));
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
fn can_reemove_messages<'a>(
    params: &InboxParams,
    signature: &Signature,
    hashes: impl Iterator<Item = &'a TokenAssignmentHash>,
) -> Result<(), ContractError> {
    let mut signed = Vec::with_capacity(hashes.size_hint().0 * 32);
    for hash in hashes {
        signed.extend(hash);
    }
    let verifying_key = VerifyingKey::<Sha256>::new(params.pub_key.clone());
    verifying_key
        .verify(
            signed.as_ref(),
            &rsa::pkcs1v15::Signature::try_from(&**signature).unwrap(),
        )
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
        .verify(
            &serialized,
            &rsa::pkcs1v15::Signature::try_from(&**signature).unwrap(),
        )
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
                        can_reemove_messages(&params, &signature, ids.iter())?;
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
                (!allocation_records.contains_key(&missing)).then_some(RelatedContract {
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
            //inbox.last_update = freenet_stdlib::time::now();
            let serialized = inbox.serialize()?;
            Ok(UpdateModification::valid(serialized.into()))
        } else {
            Ok(UpdateModification::requires(missing_related)?)
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
    use rsa::{rand_core::OsRng, Pkcs1v15Sign, RsaPrivateKey};

    #[test]
    fn validate_test() -> Result<(), Box<dyn std::error::Error>> {
        let private_key = RsaPrivateKey::new(&mut OsRng, 32).unwrap();
        let public_key = private_key.to_public_key();

        let params: Parameters = InboxParams {
            pub_key: public_key.clone(),
        }
        .try_into()
        .map_err(|e| format!("{e}"))
        .unwrap();

        use freenet_stdlib::prelude::blake3::traits::digest::Digest;
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

        let verifying_key = VerifyingKey::<Sha256>::new(public_key);
        let sign = &rsa::pkcs1v15::Signature::try_from(signature.as_slice()).unwrap();

        match verifying_key.verify(STATE_UPDATE, sign) {
            Ok(_) => println!("successful verification"),
            Err(e) => println!("verification error: {:?}", e),
        };

        let is_valid = Inbox::validate_state(params, State::from(state_bytes), Default::default())?;
        assert!(is_valid == ValidateResult::Valid);
        Ok(())
    }
}
