use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier};
use locutus_aft_interface::{Tier, TokenAllocationRecord, TokenAssignment, TokenAssignmentHash};
use locutus_stdlib::prelude::{blake2::Digest, *};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct InboxParams {
    // The public key of the inbox owner message.
    pub_key: ed25519_dalek::PublicKey,
}

impl TryFrom<Parameters<'static>> for InboxParams {
    type Error = ContractError;
    fn try_from(params: Parameters<'static>) -> Result<Self, Self::Error> {
        bincode::deserialize(params.as_ref()).map_err(|err| ContractError::Deser(format!("{err}")))
    }
}

#[derive(Serialize, Deserialize)]
enum UpdateInbox {
    AddMessages(Vec<Message>),
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
struct InboxSettings {
    minimum_tier: Tier,
}

impl TryFrom<StateDelta<'static>> for UpdateInbox {
    type Error = ContractError;
    fn try_from(state: StateDelta<'static>) -> Result<Self, Self::Error> {
        bincode::deserialize(&state).map_err(|err| ContractError::Deser(format!("{err}")))
    }
}

/// Requires a shared key between both peers to decrypt the secrets.
/// The inbox contract does not need to be aware of the content.
type EncryptedContent = Vec<u8>;

#[derive(Serialize, Deserialize)]
struct Message {
    title: EncryptedContent,
    content: EncryptedContent,
    token_assignment: TokenAssignment,
    time: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct Inbox {
    messages: Vec<Message>,
    last_update: DateTime<Utc>,
    settings: InboxSettings,
}

enum VerificationError {
    MissingContracts(Vec<ContractInstanceId>),
    TokenAssignmentMismatch,
    InvalidInboxKey,
    InvalidMessageHash,
}

impl Inbox {
    fn add_messages(
        &mut self,
        params: &InboxParams,
        allocation_records: &HashMap<ContractInstanceId, TokenAllocationRecord>,
        messages: Vec<Message>,
    ) -> Result<(), VerificationError> {
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
            // hasher.update(&message.title);
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

    fn other_messages(&self, delta: Inbox) -> impl Iterator<Item = Message> + '_ {
        let hashes: HashSet<_> = self
            .messages
            .iter()
            .map(|m| &m.token_assignment.assignment_hash)
            .collect();
        delta.messages.into_iter().filter_map(move |m| {
            (hashes.contains(&m.token_assignment.assignment_hash)).then_some(m)
        })
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
        let serialized = bincode::serialize(&InboxSummary(messages))
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
        }
    }
}

impl TryFrom<State<'static>> for Inbox {
    type Error = ContractError;
    fn try_from(state: State<'static>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&state).map_err(|err| ContractError::Deser(format!("{err}")))
    }
}

impl TryFrom<Inbox> for StateDelta<'static> {
    type Error = ContractError;
    fn try_from(value: Inbox) -> Result<Self, Self::Error> {
        let serialized =
            bincode::serialize(&value).map_err(|err| ContractError::Deser(format!("{err}")))?;
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

fn can_remove_message<'a>(
    params: &InboxParams,
    signature: &Signature,
    ids: impl Iterator<Item = &'a TokenAssignmentHash>,
) -> Result<(), ContractError> {
    for hash in ids {
        params
            .pub_key
            .verify(hash, signature)
            .map_err(|_err| ContractError::InvalidUpdate)?;
    }
    Ok(())
}

fn can_update_settings(
    params: &InboxParams,
    signature: &Signature,
    settings: &InboxSettings,
) -> Result<(), ContractError> {
    let serialized =
        serde_json::to_vec(settings).map_err(|e| ContractError::Deser(format!("{e}")))?;
    params
        .pub_key
        .verify(&serialized, signature)
        .map_err(|_err| ContractError::InvalidUpdate)?;
    Ok(())
}

#[contract]
impl ContractInterface for Inbox {
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let inbox = Inbox::try_from(state)?;
        let params = InboxParams::try_from(parameters)?;

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
        // todo: take care of race condition between token alloc record and the assignment
        let mut inbox = Inbox::try_from(state)?;
        let params = InboxParams::try_from(parameters)?;
        let mut missing_related = vec![];
        let mut new_messages = vec![];
        let mut rm_messages = HashSet::new();
        let mut allocation_records = HashMap::new();
        for update in updates {
            match update {
                UpdateData::State(state) => {
                    let delta_inbox = Inbox::try_from(state)?;
                    let delta_messages = inbox.other_messages(delta_inbox);
                    new_messages.extend(delta_messages);
                }
                UpdateData::Delta(d) => match UpdateInbox::try_from(d)? {
                    UpdateInbox::AddMessages(mut messages) => {
                        for m in &messages {
                            missing_related.push(m.token_assignment.token_record);
                        }
                        new_messages.append(&mut messages);
                    }
                    UpdateInbox::RemoveMessages { signature, ids } => {
                        can_remove_message(&params, &signature, ids.iter())?;
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
                UpdateData::StateAndDelta { state, delta } => {
                    match UpdateInbox::try_from(delta)? {
                        UpdateInbox::AddMessages(mut messages) => {
                            for m in &messages {
                                missing_related.push(m.token_assignment.token_record);
                            }
                            new_messages.append(&mut messages);
                        }
                        UpdateInbox::RemoveMessages { signature, ids } => {
                            can_remove_message(&params, &signature, ids.iter())?;
                            rm_messages.extend(ids);
                        }
                        UpdateInbox::ModifySettings {
                            settings,
                            signature,
                        } => {
                            can_update_settings(&params, &signature, &settings)?;
                            inbox.settings = settings;
                        }
                    }
                    let delta_inbox = Inbox::try_from(state)?;
                    let delta_messages = inbox.other_messages(delta_inbox);
                    new_messages.extend(delta_messages);
                }
                UpdateData::RelatedState { related_to, state } => {
                    let token_record = TokenAllocationRecord::try_from(state)?;
                    allocation_records.insert(related_to, token_record);
                }
                UpdateData::RelatedDelta { .. } => unreachable!(),
                UpdateData::RelatedStateAndDelta { .. } => unreachable!(),
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
        let inbox = Inbox::try_from(state)?;
        inbox.summarize()
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        let inbox = Inbox::try_from(state)?;
        let summary = InboxSummary::try_from(summary)?;
        let delta = inbox.delta(summary);
        delta.try_into()
    }
}
