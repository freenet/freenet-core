use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use locutus_aft_interface::{Tier, TokenAllocationRecord, TokenAssignment, TokenAssignmentHash};
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
        signature: Signature,
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

#[derive(Serialize, Deserialize, Clone)]
pub struct InboxSettings {
    pub minimum_tier: Tier,
    #[serde(skip_serializing_if = "Vec::is_empty")]
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

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub content: EncryptedContent,
    pub token_assignment: TokenAssignment,
}

#[derive(Serialize, Deserialize)]
pub struct Inbox {
    pub messages: Vec<Message>,
    pub last_update: DateTime<Utc>,
    pub settings: InboxSettings,
    inbox_signature: Signature,
}

enum VerificationError {
    MissingContracts(Vec<ContractInstanceId>),
    TokenAssignmentMismatch,
    InvalidInboxKey,
    InvalidMessageHash,
    WrongSignature,
}

impl From<VerificationError> for ContractError {
    fn from(_err: VerificationError) -> Self {
        ContractError::InvalidUpdate
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
        let verifying_key = VerifyingKey::<Sha256>::from(params.pub_key.clone());
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
        params: &InboxParams,
        allocation_records: &HashMap<ContractInstanceId, TokenAllocationRecord>,
        messages: Vec<Message>,
    ) -> Result<(), VerificationError> {
        // FIXME: make sure we are not re-adding old messages by verifying the time is moe recent
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
            let mut hasher = blake2::Blake2s256::new();
            hasher.update(&message.content);
            let hash = hasher.finalize();
            if message.token_assignment.assignment_hash != hash.as_slice() {
                return Err(VerificationError::InvalidMessageHash);
            }
            self.add_message(message, params)?;
        }
        Ok(())
    }

    fn verify_messages(
        &self,
        params: &InboxParams,
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
            (message.token_assignment.assignee == params.pub_key)
                .then_some(())
                .ok_or(VerificationError::InvalidInboxKey)?;
        }
        if !missing.is_empty() {
            return Err(VerificationError::MissingContracts(missing));
        }
        Ok(())
    }

    fn add_message(
        &mut self,
        message: Message,
        params: &InboxParams,
    ) -> Result<(), VerificationError> {
        (message.token_assignment.assignee == params.pub_key)
            .then_some(())
            .ok_or(VerificationError::InvalidInboxKey)?;
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

        match inbox.verify_messages(&params, &allocation_records) {
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
                UpdateData::Delta(d) => match UpdateInbox::try_from(d)? {
                    UpdateInbox::AddMessages {
                        mut messages,
                        signature,
                    } => {
                        // FIXME: should the msg be verified for the pub key in the token assignment (the one sending the msg)
                        // instead of the one owning the inbox?
                        can_modify_inbox(
                            &params,
                            &signature,
                            messages.iter().map(|m| &m.token_assignment.assignment_hash),
                        )?;
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
                .add_messages(&params, &allocation_records, new_messages)
                .map_err(|_| ContractError::InvalidUpdate)?;
            inbox.remove_messages(rm_messages);
            inbox.last_update = locutus_stdlib::time::now();
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
        let inbox = Inbox::try_from(&state)?;
        let summary = InboxSummary::try_from(summary)?;
        let delta = inbox.delta(summary);
        delta.try_into()
    }
}
