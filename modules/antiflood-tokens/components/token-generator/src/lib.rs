use chrono::{DateTime, Utc};
use ed25519_dalek::{Keypair, Signer};
use locutus_aft_interface::{AllocationCriteria, Tier, TokenAllocationRecord, TokenAssignment};
use locutus_stdlib::{prelude::*, time};
use serde::{Deserialize, Serialize};

type Assignee = Vec<u8>;

struct TokenComponent;

#[derive(Debug, Serialize, Deserialize)]
enum TokenComponentMessage {
    RequestNewToken {
        component_id: SecretsId,
        criteria: AllocationCriteria,
        records: TokenAllocationRecord,
        assignee: Assignee,
        context: Option<Context>,
    },
}

impl TokenComponentMessage {
    fn add_context(&mut self, new_context: Context) {
        match self {
            TokenComponentMessage::RequestNewToken { context, .. } => {
                context.replace(new_context);
            }
        }
    }
}

impl TryFrom<TokenComponentMessage> for OutboundComponentMsg {
    type Error = ComponentError;
    fn try_from(msg: TokenComponentMessage) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Context {
    /// The token generator instance key pair (pub + secret key).
    key_pair: Option<Keypair>,
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

impl<'a> TryFrom<InboundComponentMsg<'a>> for TokenComponentMessage {
    type Error = ComponentError;

    fn try_from(msg: InboundComponentMsg) -> Result<Self, Self::Error> {
        match msg {
            InboundComponentMsg::ApplicationMessage(ApplicationMessage {
                app,
                payload,
                processed,
                context,
                ..
            }) => {
                // todo: check that the contract instance id is matching?
                if processed {
                    return Err(ComponentError::Other(
                        "message should have not been previously processed".into(),
                    ));
                }
                let context = Context::try_from(context)?;
                let mut msg: TokenComponentMessage = bincode::deserialize(&payload)
                    .map_err(|err| ComponentError::Deser(format!("{err}")))?;
                msg.add_context(context);
                Ok(msg)
            }
            InboundComponentMsg::GetSecretResponse(_) => todo!(),
            InboundComponentMsg::RandomBytes(_) => todo!(),
            InboundComponentMsg::UserResponse(_) => todo!(),
        }
    }
}

#[component]
impl ComponentInterface for TokenComponent {
    fn process(message: InboundComponentMsg) -> Result<Vec<OutboundComponentMsg>, ComponentError> {
        let msg = TokenComponentMessage::try_from(message)?;
        match msg {
            TokenComponentMessage::RequestNewToken {
                component_id,
                criteria,
                records,
                assignee,
                context,
            } => {
                let ctx = context.as_ref().unwrap();
                if let Some(keys) = &ctx.key_pair {
                    request_allocation(criteria, assignee, records, keys);
                } else {
                    let request_secret = {
                        let context = (&Context::default()).try_into()?;
                        GetSecretRequest {
                            key: component_id.clone(),
                            context,
                            processed: false,
                        }
                        .into()
                    };
                    let allocate_msg = TokenComponentMessage::RequestNewToken {
                        component_id,
                        criteria,
                        records,
                        assignee,
                        context,
                    };
                    return Ok(vec![request_secret, allocate_msg.try_into()?]);
                }
            }
        }
        todo!()
    }
}

fn request_allocation(
    criteria: AllocationCriteria,
    assignee: Assignee,
    mut records: TokenAllocationRecord,
    keys: &Keypair,
) {
    let tier = criteria.frequency;
    records.assign(assignee, tier, keys);
    todo!()
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
    fn assign(&mut self, assignee: Assignee, tier: Tier, private_key: &Keypair);
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
    fn assign(&mut self, assignee: Assignee, tier: Tier, keys: &Keypair) {
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
        self.append_unchecked(assignment);
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
