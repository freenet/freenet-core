use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use locutus_aft_interface::{AllocationCriteria, TokenAllocationRecord, TokenAssignment};
use locutus_stdlib::{prelude::*, time};
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use rsa::{
    pkcs1v15::SigningKey, pkcs8::EncodePublicKey, sha2::Sha256, RsaPrivateKey, RsaPublicKey,
};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

type Assignee = RsaPublicKey;

type AssignmentHash = [u8; 32];

struct TokenComponent;

#[component]
impl ComponentInterface for TokenComponent {
    fn process(message: InboundComponentMsg) -> Result<Vec<OutboundComponentMsg>, ComponentError> {
        match message {
            InboundComponentMsg::ApplicationMessage(ApplicationMessage {
                app,
                payload,
                context,
                processed,
                ..
            }) => {
                if processed {
                    return Err(ComponentError::Other(
                        "cannot process an already processed message".into(),
                    ));
                }
                let mut context = Context::try_from(context)?;
                let msg = TokenComponentMessage::try_from(&*payload)?;
                match msg {
                    TokenComponentMessage::RequestNewToken(req) => {
                        allocate_token(&mut context, app, req)
                    }
                    TokenComponentMessage::Failure { .. } => Err(ComponentError::Other(
                        "unexpected message type: failure".into(),
                    )),
                    TokenComponentMessage::AllocatedToken { .. } => Err(ComponentError::Other(
                        "unexpected message type: allocated token".into(),
                    )),
                }
            }
            InboundComponentMsg::UserResponse(UserInputResponse {
                request_id,
                response,
                context,
            }) => {
                let mut context = Context::try_from(context)?;
                context.waiting_for_user_input.remove(&request_id);
                let response = serde_json::from_slice(&response)
                    .map_err(|err| ComponentError::Deser(format!("{err}")))?;
                context.user_response.insert(request_id, response);
                let context: ComponentContext = (&context).try_into()?;
                Ok(vec![OutboundComponentMsg::ContextUpdated(context)])
            }
            InboundComponentMsg::GetSecretResponse(GetSecretResponse {
                key,
                value,
                context,
            }) => {
                let mut context = Context::try_from(context)?;
                let secret = value.ok_or_else(|| {
                    ComponentError::Other(format!("secret not found for key: {key}"))
                })?;
                let keypair: RsaPrivateKey = bincode::deserialize(&secret)
                    .map_err(|err| ComponentError::Deser(format!("{err}")))?;
                context.key_pair = Some(keypair);
                let context: ComponentContext = (&context).try_into()?;
                Ok(vec![OutboundComponentMsg::ContextUpdated(context)])
            }
            InboundComponentMsg::RandomBytes(_) => Err(ComponentError::Other(
                "unexpected message type: radom bytes".into(),
            )),
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
    context: &mut Context,
    app: ContractInstanceId,
    RequestNewToken {
        request_id,
        component_id,
        criteria,
        mut records,
        assignee,
        assignment_hash,
    }: RequestNewToken,
) -> Result<Vec<OutboundComponentMsg>, ComponentError> {
    let request = context
        .waiting_for_user_input
        .iter()
        .find(|p| **p == request_id)
        .copied();
    let response = context.user_response.get(&request_id);
    match (&context.key_pair, request, response) {
        (None, _, _) => {
            // lacks keys; ask for keys
            let context: ComponentContext = (&*context).try_into()?;
            let request_secret = {
                GetSecretRequest {
                    key: component_id.clone(),
                    context: context.clone(),
                    processed: false,
                }
                .into()
            };
            let req_allocation = {
                let msg = TokenComponentMessage::RequestNewToken(RequestNewToken {
                    request_id,
                    component_id,
                    criteria,
                    records,
                    assignee,
                    assignment_hash,
                });
                OutboundComponentMsg::ApplicationMessage(
                    ApplicationMessage::new(app, msg.serialize()?, false).with_context(context),
                )
            };
            Ok(vec![request_secret, req_allocation])
        }
        (Some(_), None, None) => {
            // request user input and add to waiting queue
            context.waiting_for_user_input.insert(request_id);
            let context: ComponentContext = (&*context).try_into()?;
            let message = user_input(&criteria, &assignee);
            let req_allocation = {
                let msg = TokenComponentMessage::RequestNewToken(RequestNewToken {
                    request_id,
                    component_id,
                    criteria,
                    records,
                    assignee,
                    assignment_hash,
                });
                OutboundComponentMsg::ApplicationMessage(
                    ApplicationMessage::new(app, msg.serialize()?, false).with_context(context),
                )
            };
            let request_user_input = OutboundComponentMsg::RequestUserInput(UserInputRequest {
                request_id,
                responses: RESPONSES
                    .iter()
                    .map(|s| ClientResponse::new(s.as_bytes().to_vec()))
                    .collect(),
                message,
            });
            Ok(vec![request_user_input, req_allocation])
        }
        (Some(_), Some(_), _) => {
            // waiting for response
            let context: ComponentContext = (&*context).try_into()?;
            let req_allocation = {
                let msg = TokenComponentMessage::RequestNewToken(RequestNewToken {
                    request_id,
                    component_id,
                    criteria,
                    records,
                    assignee,
                    assignment_hash,
                });
                OutboundComponentMsg::ApplicationMessage(
                    ApplicationMessage::new(app, msg.serialize()?, false).with_context(context),
                )
            };
            Ok(vec![req_allocation])
        }
        (Some(keypair), _, Some(response)) => {
            // got response, check if allocation is allowed and return to application
            let application_response = match response {
                Response::Allowed => {
                    let context: ComponentContext = (&*context).try_into()?;
                    let Some(assignment) = records.assign(assignee, &criteria, keypair, assignment_hash) else {
                        let msg = TokenComponentMessage::Failure(FailureReason::NoFreeSlot { component_id, criteria } );
                        return Ok(vec![OutboundComponentMsg::ApplicationMessage(
                            ApplicationMessage::new(app, msg.serialize()?, true).with_context(context),
                        )]);
                    };
                    let msg = TokenComponentMessage::AllocatedToken {
                        component_id,
                        assignment,
                        records,
                    };
                    OutboundComponentMsg::ApplicationMessage(
                        ApplicationMessage::new(app, msg.serialize()?, true).with_context(context),
                    )
                }
                Response::NotAllowed => {
                    let context: ComponentContext = (&*context).try_into()?;
                    let msg = TokenComponentMessage::Failure(FailureReason::UserPermissionDenied);
                    OutboundComponentMsg::ApplicationMessage(
                        ApplicationMessage::new(app, msg.serialize()?, true).with_context(context),
                    )
                }
            };
            context.user_response.remove(&request_id);
            Ok(vec![application_response])
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum TokenComponentMessage {
    RequestNewToken(RequestNewToken),
    AllocatedToken {
        component_id: SecretsId,
        assignment: TokenAssignment,
        /// An updated version of the record with the newly allocated token included
        records: TokenAllocationRecord,
    },
    Failure(FailureReason),
}

impl TokenComponentMessage {
    fn serialize(self) -> Result<Vec<u8>, ComponentError> {
        bincode::serialize(&self).map_err(|err| ComponentError::Deser(format!("{err}")))
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum FailureReason {
    /// The user didn't accept to allocate the tokens.
    UserPermissionDenied,
    /// No free slot to allocate with the requested criteria
    NoFreeSlot {
        component_id: SecretsId,
        criteria: AllocationCriteria,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestNewToken {
    request_id: u32,
    component_id: SecretsId,
    criteria: AllocationCriteria,
    records: TokenAllocationRecord,
    assignee: Assignee,
    assignment_hash: AssignmentHash,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Context {
    waiting_for_user_input: HashSet<u32>,
    user_response: HashMap<u32, Response>,
    /// The token generator instance key pair (pub + secret key).
    key_pair: Option<rsa::RsaPrivateKey>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Allowed,
    NotAllowed,
}

impl TryFrom<ComponentContext> for Context {
    type Error = ComponentError;

    fn try_from(value: ComponentContext) -> Result<Self, Self::Error> {
        bincode::deserialize(&value.0).map_err(|err| ComponentError::Deser(format!("{err}")))
    }
}

impl TryFrom<&Context> for ComponentContext {
    type Error = ComponentError;

    fn try_from(value: &Context) -> Result<Self, Self::Error> {
        bincode::serialize(value)
            .map(ComponentContext::new)
            .map_err(|err| ComponentError::Deser(format!("{err}")))
    }
}

impl TryFrom<&[u8]> for TokenComponentMessage {
    type Error = ComponentError;

    fn try_from(payload: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(payload).map_err(|err| ComponentError::Deser(format!("{err}")))
    }
}

/// This is used internally by the component to allocate new tokens on behave of the requesting client app.
///  
/// Conflicting assignments for the same time slot are not permitted and indicate that the generator is broken or malicious.
trait TokenAssignmentInternal {
    fn assign(
        &mut self,
        assignee: Assignee,
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
        assignee: Assignee,
        criteria: &AllocationCriteria,
        key: &RsaPrivateKey,
        assignment_hash: AssignmentHash,
    ) -> Option<TokenAssignment> {
        use rsa::signature::RandomizedSigner;
        let current = time::now();
        let time_slot = self.next_free_assignment(criteria, current)?;
        let assignment = {
            let msg = TokenAssignment::to_be_signed(&time_slot, &assignee, criteria.frequency);
            let signing_key = SigningKey::<Sha256>::new_with_prefix(key.clone());
            // FIXME: must change fopr a random seed from locutus_stdlib::rand::* ; just a quick workaround
            let mut rng = ChaChaRng::seed_from_u64(1);
            let signature = signing_key.sign_with_rng(&mut rng, &msg);
            TokenAssignment {
                tier: criteria.frequency,
                time_slot,
                assignee,
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
