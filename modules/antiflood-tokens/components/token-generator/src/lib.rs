use chrono::{DateTime, Utc};
use ed25519_dalek::{Keypair, Signer};
use hashbrown::{HashMap, HashSet};
use locutus_aft_interface::{AllocationCriteria, Tier, TokenAllocationRecord, TokenAssignment};
use locutus_stdlib::{prelude::*, time};
use serde::{Deserialize, Serialize};

type Assignee = Vec<u8>;

struct TokenComponent;

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
                    TokenComponentMessage::Failure => todo!(),
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
                let keypair = Keypair::from_bytes(&secret)
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

fn user_input() -> NotificationMessage<'static> {
    todo!()
}

fn allocate_token(
    context: &mut Context,
    app: ContractInstanceId,
    req: RequestNewToken,
) -> Result<Vec<OutboundComponentMsg>, ComponentError> {
    let RequestNewToken {
        request_id,
        component_id,
        criteria,
        mut records,
        assignee,
    } = req;
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
                });
                let serialized = bincode::serialize(&msg)
                    .map_err(|err| ComponentError::Deser(format!("{err}")))?;
                OutboundComponentMsg::ApplicationMessage(
                    ApplicationMessage::new(app, serialized, false).with_context(context),
                )
            };
            Ok(vec![request_secret, req_allocation])
        }
        (Some(_), None, None) => {
            // request user input and add to waiting queue
            context.waiting_for_user_input.insert(request_id);
            let context: ComponentContext = (&*context).try_into()?;
            let req_allocation = {
                let msg = TokenComponentMessage::RequestNewToken(RequestNewToken {
                    request_id,
                    component_id,
                    criteria,
                    records,
                    assignee,
                });
                let serialized = bincode::serialize(&msg)
                    .map_err(|err| ComponentError::Deser(format!("{err}")))?;
                OutboundComponentMsg::ApplicationMessage(
                    ApplicationMessage::new(app, serialized, false).with_context(context),
                )
            };
            let request_user_input = OutboundComponentMsg::RequestUserInput(UserInputRequest {
                request_id,
                responses: RESPONSES
                    .iter()
                    .map(|s| ClientResponse::new(s.as_bytes().to_vec()))
                    .collect(),
                message: user_input(),
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
                });
                let serialized = bincode::serialize(&msg)
                    .map_err(|err| ComponentError::Deser(format!("{err}")))?;
                OutboundComponentMsg::ApplicationMessage(
                    ApplicationMessage::new(app, serialized, false).with_context(context),
                )
            };
            Ok(vec![req_allocation])
        }
        (Some(keypair), _, Some(response)) => {
            // got response, check if allocation is allowed and return to application
            let application_response = match response {
                Response::Allowed => {
                    let assignment = records.assign(assignee, criteria.frequency, keypair);
                    let context: ComponentContext = (&*context).try_into()?;
                    let msg = TokenComponentMessage::AllocatedToken {
                        component_id,
                        assignment,
                        records,
                    };
                    let serialized = bincode::serialize(&msg)
                        .map_err(|err| ComponentError::Deser(format!("{err}")))?;
                    OutboundComponentMsg::ApplicationMessage(
                        ApplicationMessage::new(app, serialized, true).with_context(context),
                    )
                }
                Response::NotAllowed => todo!("return to application communicating the denial"),
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
    Failure,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestNewToken {
    request_id: u32,
    component_id: SecretsId,
    criteria: AllocationCriteria,
    records: TokenAllocationRecord,
    assignee: Assignee,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Context {
    waiting_for_user_input: HashSet<u32>,
    user_response: HashMap<u32, Response>,
    /// The token generator instance key pair (pub + secret key).
    key_pair: Option<Keypair>,
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

// TODO: clarify where this functionality would be called, e.g.:
// - does the application request a token and then appends the returned token if successful
// - or instead, the application requests a token and this is appended in place by the component and returns an updated component
/// This should be used by applications to assign tokens previously requested to this component.
pub trait TokenAssignmentExt {
    fn append_unchecked(&mut self, assignment: TokenAssignment);
}

/// This is used internally by the component to allocate new tokens on behave of the requesting client app.
///  
/// Conflicting assignments for the same time slot are not permitted and indicate that the generator is broken or malicious.
trait TokenAssignmentInternal {
    fn assign(&mut self, assignee: Assignee, tier: Tier, private_key: &Keypair) -> TokenAssignment;
    fn next_free_assignment(&self, tier: Tier) -> DateTime<Utc>;
}

impl TokenAssignmentExt for TokenAllocationRecord {
    fn append_unchecked(&mut self, assignment: TokenAssignment) {
        match self.tokens_by_tier.get_mut(&assignment.tier) {
            Some(list) => {
                list.push(assignment);
                list.sort_unstable();
            }
            None => {
                self.tokens_by_tier
                    .insert(assignment.tier, vec![assignment]);
            }
        }
    }
}

impl TokenAssignmentInternal for TokenAllocationRecord {
    /// Assigns the next theoretical free slot. This could be out of sync due to other concurrent requests so it may fail
    /// to validate at the node. In that case the application should retry again, after refreshing the ledger version.
    fn assign(&mut self, assignee: Assignee, tier: Tier, keys: &Keypair) -> TokenAssignment {
        let time_slot = self.next_free_assignment(tier);
        let assignment = {
            let msg = TokenAssignment::to_be_signed(&time_slot, &assignee, tier);
            let signature = keys.sign(&msg);
            TokenAssignment {
                tier,
                time_slot,
                assignee,
                signature,
            }
        };
        self.append_unchecked(assignment.clone());
        assignment
    }

    fn next_free_assignment(&self, tier: Tier) -> DateTime<Utc> {
        let now = time::now();
        match self.tokens_by_tier.get(&tier) {
            Some(_others) => {
                //
                todo!()
            }
            None => tier.next_assignment(now),
        }
    }
}
