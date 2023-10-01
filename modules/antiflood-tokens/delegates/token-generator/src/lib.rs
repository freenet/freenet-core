use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use freenet_aft_interface::*;
use freenet_stdlib::prelude::*;
use rsa::{pkcs8::EncodePublicKey, RsaPrivateKey};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

struct TokenDelegate;

#[delegate]
impl DelegateInterface for TokenDelegate {
    fn process(
        params: Parameters<'static>,
        _attested: Option<&'static [u8]>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match message {
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage {
                app,
                payload,
                context,
                processed,
                ..
            }) => {
                if processed {
                    return Err(DelegateError::Other(
                        "cannot process an already processed message".into(),
                    ));
                }
                let mut context = Context::try_from(context)?;
                let msg = TokenDelegateMessage::try_from(&*payload)?;
                let params = DelegateParameters::try_from(params)?;
                match msg {
                    TokenDelegateMessage::RequestNewToken(req) => {
                        allocate_token(params, &mut context, app, req)
                    }
                    TokenDelegateMessage::Failure(reason) => Err(DelegateError::Other(format!(
                        "unexpected message type: failure; reason: {reason}"
                    ))),
                    TokenDelegateMessage::AllocatedToken { .. } => Err(DelegateError::Other(
                        "unexpected message type: allocated token".into(),
                    )),
                }
            }
            InboundDelegateMsg::UserResponse(UserInputResponse {
                request_id,
                response,
                context,
            }) => {
                let mut context = Context::try_from(context)?;
                context.waiting_for_user_input.remove(&request_id);
                let response = serde_json::from_slice(&response)
                    .map_err(|err| DelegateError::Deser(format!("{err}")))?;
                context.user_response.insert(request_id, response);
                let context: DelegateContext = (&context).try_into()?;
                Ok(vec![OutboundDelegateMsg::ContextUpdated(context)])
            }
            InboundDelegateMsg::GetSecretResponse(GetSecretResponse { .. }) => Err(
                DelegateError::Other("unexpected message type: get secret".into()),
            ),
            InboundDelegateMsg::GetSecretRequest(_) => unreachable!(),
        }
    }
}

const RESPONSES: &[&str] = &["true", "false"];

fn user_input(criteria: &AllocationCriteria, assignee: &Assignee) -> NotificationMessage<'static> {
    let assignee = assignee
        .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap();
    let notification_json = serde_json::json!({
        "user": assignee,
        "token": {
            "tier": criteria.frequency,
            "max_age": format!("{} seconds", criteria.max_age.as_secs())
        }
    });
    NotificationMessage::try_from(&notification_json).unwrap()
}

fn allocate_token(
    params: DelegateParameters,
    context: &mut Context,
    app: ContractInstanceId,
    RequestNewToken {
        request_id,
        delegate_id,
        criteria,
        mut records,
        assignment_hash,
    }: RequestNewToken,
) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
    let request = context
        .waiting_for_user_input
        .iter()
        .find(|p| **p == request_id)
        .copied();
    let response = context.user_response.get(&request_id);
    match (request, response) {
        (None, None) => {
            // request user input and add to waiting queue
            context.waiting_for_user_input.insert(request_id);
            let context: DelegateContext = (&*context).try_into()?;
            let message = user_input(&criteria, &params.generator_private_key.to_public_key());
            let req_allocation = {
                let msg = TokenDelegateMessage::RequestNewToken(RequestNewToken {
                    request_id,
                    delegate_id,
                    criteria,
                    records,
                    assignment_hash,
                });
                OutboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(app, msg.serialize()?).with_context(context),
                )
            };
            let request_user_input = OutboundDelegateMsg::RequestUserInput(UserInputRequest {
                request_id,
                responses: RESPONSES
                    .iter()
                    .map(|s| ClientResponse::new(s.as_bytes().to_vec()))
                    .collect(),
                message,
            });
            Ok(vec![request_user_input, req_allocation])
        }
        (Some(_), _) => {
            // waiting for response
            let context: DelegateContext = (&*context).try_into()?;
            let req_allocation = {
                let msg = TokenDelegateMessage::RequestNewToken(RequestNewToken {
                    request_id,
                    delegate_id,
                    criteria,
                    records,
                    assignment_hash,
                });
                OutboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(app, msg.serialize()?).with_context(context),
                )
            };
            Ok(vec![req_allocation])
        }
        (_, Some(response)) => {
            // got response, check if allocation is allowed and return to application
            let application_response = match response {
                Response::Allowed => {
                    let context: DelegateContext = (&*context).try_into()?;
                    let Some(assignment) =
                        records.assign(&criteria, &params.generator_private_key, assignment_hash)
                    else {
                        let msg = TokenDelegateMessage::Failure(FailureReason::NoFreeSlot {
                            delegate_id,
                            criteria,
                        });
                        return Ok(vec![OutboundDelegateMsg::ApplicationMessage(
                            ApplicationMessage::new(app, msg.serialize()?).with_context(context),
                        )]);
                    };
                    let msg = TokenDelegateMessage::AllocatedToken {
                        delegate_id,
                        assignment,
                        records,
                    };
                    OutboundDelegateMsg::ApplicationMessage(
                        ApplicationMessage::new(app, msg.serialize()?)
                            .processed(true)
                            .with_context(context),
                    )
                }
                Response::NotAllowed => {
                    let context: DelegateContext = (&*context).try_into()?;
                    let msg = TokenDelegateMessage::Failure(FailureReason::UserPermissionDenied);
                    OutboundDelegateMsg::ApplicationMessage(
                        ApplicationMessage::new(app, msg.serialize()?).with_context(context),
                    )
                }
            };
            context.user_response.remove(&request_id);
            Ok(vec![application_response])
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Context {
    waiting_for_user_input: HashSet<u32>,
    user_response: HashMap<u32, Response>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Allowed,
    NotAllowed,
}

impl TryFrom<DelegateContext> for Context {
    type Error = DelegateError;

    fn try_from(value: DelegateContext) -> Result<Self, Self::Error> {
        if value == DelegateContext::default() {
            return Ok(Self::default());
        }
        bincode::deserialize(value.as_ref()).map_err(|err| DelegateError::Deser(format!("{err}")))
    }
}

impl TryFrom<&Context> for DelegateContext {
    type Error = DelegateError;

    fn try_from(value: &Context) -> Result<Self, Self::Error> {
        bincode::serialize(value)
            .map(DelegateContext::new)
            .map_err(|err| DelegateError::Deser(format!("{err}")))
    }
}

/// This is used internally by the delegate to allocate new tokens on behave of the requesting client app.
///  
/// Conflicting assignments for the same time slot are not permitted and indicate that the generator is broken or malicious.
trait TokenAssignmentInternal {
    fn assign(
        &mut self,
        criteria: &AllocationCriteria,
        private_key: &RsaPrivateKey,
        assignment_hash: AssignmentHash,
    ) -> Option<TokenAssignment>;

    /// Given a datetime, get the newest free slot for this criteria.
    fn next_free_assignment(
        &self,
        criteria: &AllocationCriteria,
        current: DateTime<Utc>,
    ) -> Option<DateTime<Utc>>;

    fn append_unchecked(&mut self, assignment: TokenAssignment);
}

impl TokenAssignmentInternal for TokenAllocationRecord {
    /// Assigns the next theoretical free slot. This could be out of sync due to other concurrent requests so it may fail
    /// to validate at the node. In that case the application should retry again, after refreshing the ledger version.
    fn assign(
        &mut self,
        criteria: &AllocationCriteria,
        generator_pk: &RsaPrivateKey,
        assignment_hash: AssignmentHash,
    ) -> Option<TokenAssignment> {
        use rsa::pkcs1v15::SigningKey;
        use rsa::sha2::Sha256;
        use rsa::signature::Signer;
        #[cfg(target_family = "wasm")]
        #[inline(always)]
        fn current() -> DateTime<Utc> {
            freenet_stdlib::time::now()
        }
        #[cfg(not(target_family = "wasm"))]
        #[inline(always)]
        fn current() -> DateTime<Utc> {
            Utc::now()
        }
        let current = current();
        let time_slot = self.next_free_assignment(criteria, current)?;
        let assignment = {
            let msg = TokenAssignment::signature_content(
                &time_slot,
                criteria.frequency,
                &assignment_hash,
            );
            let signing_key = SigningKey::<Sha256>::new(generator_pk.clone());
            let signature = signing_key.sign(&msg);
            #[cfg(target_family = "wasm")]
            {
                let pk = generator_pk
                    .to_public_key()
                    .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
                    .unwrap()
                    .split_whitespace()
                    .collect::<String>();
                // freenet_stdlib::log::info(&format!(
                //     "signed message {msg:?} with pub key: `{pk}`, signature: {signature}"
                // ));
            }
            TokenAssignment {
                tier: criteria.frequency,
                time_slot,
                generator: generator_pk.to_public_key(),
                signature,
                assignment_hash,
                token_record: criteria.contract,
            }
        };
        self.append_unchecked(assignment.clone());
        Some(assignment)
    }

    fn next_free_assignment(
        &self,
        criteria: &AllocationCriteria,
        current: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let normalized = criteria.frequency.normalize_to_next(current);
        let max_age = Duration::from_std(criteria.max_age).unwrap();
        // todo: add optimization branch where we check if all slots have been allocated upfront
        match self.get_tier(&criteria.frequency) {
            Some(currently_assigned) => {
                let mut oldest_valid_observed = None;
                let mut first_valid = None;
                for (_idx, assignment) in currently_assigned.iter().enumerate() {
                    // dbg!(
                    //     oldest_valid_observed.map(|a: &TokenAssignment| a.time_slot),
                    //     first_valid,
                    //     assignment.time_slot
                    // );
                    if assignment.time_slot >= (normalized - max_age) {
                        match (oldest_valid_observed, first_valid) {
                            (None, None) => {
                                oldest_valid_observed = Some(assignment);
                                let oldest_valid = normalized - max_age;
                                if assignment.time_slot != oldest_valid {
                                    first_valid = Some(oldest_valid);
                                } else {
                                    let prev = assignment.previous_slot();
                                    if prev > oldest_valid {
                                        first_valid = Some(prev);
                                    } else {
                                        first_valid = Some(assignment.next_slot());
                                    }
                                }
                            }
                            (Some(_), Some(first_valid_date)) => {
                                if first_valid_date == assignment.time_slot {
                                    let next = assignment.next_slot();
                                    if next > normalized {
                                        first_valid = None;
                                    } else {
                                        first_valid = Some(next);
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                }

                if oldest_valid_observed.is_none() {
                    // first slot for the tier free
                    return Some(normalized - max_age);
                }
                if first_valid.is_some() {
                    return first_valid;
                }
                None
            }
            None => Some(normalized - max_age),
        }
    }

    fn append_unchecked(&mut self, assignment: TokenAssignment) {
        match self.get_mut_tier(&assignment.tier) {
            Some(list) => {
                list.push(assignment);
                list.sort_unstable();
            }
            None => {
                self.insert(assignment.tier, vec![assignment]);
            }
        }
    }
}
