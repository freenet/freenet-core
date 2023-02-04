use std::collections::{HashMap, HashSet};

use ed25519_dalek::{Signature, Verifier};
use locutus_aft_interface::{TokenAllocationRecord, TokenAssignment};
use locutus_stdlib::prelude::{blake2::Digest, *};
use serde::{Deserialize, Serialize};

type MessageId = u64;

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
    AddMessage(Box<Message>),
    AddMessages(Vec<Message>),
    RemoveMessage {
        signature: Signature,
        id: MessageId,
    },
    RemoveMessages {
        signature: Signature,
        ids: Vec<MessageId>,
    },
}

impl TryFrom<StateDelta<'static>> for UpdateInbox {
    type Error = ContractError;
    fn try_from(state: StateDelta<'static>) -> Result<Self, Self::Error> {
        bincode::deserialize(&state).map_err(|err| ContractError::Deser(format!("{err}")))
    }
}

#[derive(Serialize, Deserialize)]
struct MessageWithMeta {
    message: Message,
    id: u64,
}

/// Requires a shared key between both peers to decrypt the secrets.
/// The inbox contract does not need to be aware of the content.
type EncryptedContent = Vec<u8>;

#[derive(Serialize, Deserialize)]
struct Message {
    title: EncryptedContent,
    content: EncryptedContent,
    token_assignment: TokenAssignment,
    /// Key to the contract holding the token records of the assignee.
    token_record: ContractInstanceId,
}

#[derive(Serialize, Deserialize)]
struct Inbox {
    messages: Vec<MessageWithMeta>,
    msg_id_counter: u64,
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
                .get(&message.token_record)
                .ok_or_else(|| VerificationError::MissingContracts(vec![message.token_record]))?;
            if !records.assignment_exists(&message.token_assignment) {
                return Err(VerificationError::TokenAssignmentMismatch);
            }
            let mut hasher = blake2::Blake2s256::new();
            hasher.update(&message.title);
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
        for meta in &self.messages {
            let Some(records) = allocation_records.get(&meta.message.token_record) else {
                missing.push(meta.message.token_record);
                some_missing = true;
                continue;
            };
            if some_missing {
                continue;
            }
            if !records.assignment_exists(&meta.message.token_assignment) {
                return Err(VerificationError::TokenAssignmentMismatch);
            }
            (meta.message.token_assignment.assignee == params.pub_key)
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
        self.messages.push(MessageWithMeta {
            message,
            id: self.msg_id_counter,
        });
        self.msg_id_counter += 1;
        Ok(())
    }

    fn other_messages(&self, delta: Inbox) -> impl Iterator<Item = Message> {
        let counter = self.msg_id_counter;
        delta
            .messages
            .into_iter()
            .filter_map(move |m| (m.id >= counter).then_some(m.message))
    }

    fn remove_messages(&mut self, messages: HashSet<MessageId>) {
        self.messages.retain(|m| !messages.contains(&m.id));
    }

    fn summarize(self) -> Result<StateSummary<'static>, ContractError> {
        let messages = self
            .messages
            .into_iter()
            .map(|MessageWithMeta { id, .. }| id)
            .collect::<HashSet<_>>();
        let serialized = bincode::serialize(&InboxSummary(messages))
            .map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(StateSummary::from(serialized))
    }

    fn delta(self, InboxSummary(messages): InboxSummary) -> Inbox {
        let delta = self
            .messages
            .into_iter()
            .filter(|m| !messages.contains(&m.id))
            .collect();
        Inbox {
            messages: delta,
            msg_id_counter: self.msg_id_counter,
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
struct InboxSummary(HashSet<u64>);

impl TryFrom<StateSummary<'static>> for InboxSummary {
    type Error = ContractError;
    fn try_from(summary: StateSummary<'static>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&summary).map_err(|err| ContractError::Deser(format!("{err}")))
    }
}

fn can_remove_message<'a>(
    params: &InboxParams,
    signature: &Signature,
    ids: impl Iterator<Item = &'a MessageId>,
) -> Result<(), ContractError> {
    let bytes: Vec<_> = ids.flat_map(|id| id.to_le_bytes()).collect();
    params
        .pub_key
        .verify(&bytes, signature)
        .map_err(|_err| ContractError::InvalidUpdate)
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
                    UpdateInbox::AddMessage(msg) => {
                        missing_related.push(msg.token_record);
                        new_messages.push(*msg);
                    }
                    UpdateInbox::AddMessages(mut messages) => {
                        new_messages.append(&mut messages);
                    }
                    UpdateInbox::RemoveMessage { signature, id } => {
                        can_remove_message(&params, &signature, [&id].into_iter())?;
                        rm_messages.insert(id);
                    }
                    UpdateInbox::RemoveMessages { signature, ids } => {
                        can_remove_message(&params, &signature, ids.iter())?;
                        rm_messages.extend(ids);
                    }
                },
                UpdateData::StateAndDelta { state, delta } => {
                    match UpdateInbox::try_from(delta)? {
                        UpdateInbox::AddMessage(msg) => {
                            missing_related.push(msg.token_record);
                            new_messages.push(*msg);
                        }
                        UpdateInbox::AddMessages(mut messages) => {
                            new_messages.append(&mut messages);
                        }
                        UpdateInbox::RemoveMessage { signature, id } => {
                            can_remove_message(&params, &signature, [&id].into_iter())?;
                            rm_messages.insert(id);
                        }
                        UpdateInbox::RemoveMessages { signature, ids } => {
                            can_remove_message(&params, &signature, ids.iter())?;
                            rm_messages.extend(ids);
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
                    mode: RelatedMode::StateEvery,
                })
            })
            .collect();

        if missing_related.is_empty() {
            inbox
                .add_messages(&params, &allocation_records, new_messages)
                .map_err(|_| ContractError::InvalidUpdate)?;
            inbox.remove_messages(rm_messages);
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
