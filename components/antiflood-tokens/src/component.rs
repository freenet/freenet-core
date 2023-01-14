use locutus_stdlib::prelude::*;

use super::*;

struct TokenComponent;

enum TokenComponentMessage {}

#[component]
impl ComponentInterface for TokenComponent {
    fn process(messages: InboundComponentMsg) -> Result<Vec<OutboundComponentMsg>, ComponentError> {
        todo!()
    }
}

/// Conflicting assignments for the same time slot are not permitted and indicate that the generator is broken or malicious.
impl TokenAssignmentLedger {
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

    /// Assigns the next theoretical free slot. This could be out of sync due to other concurrent requests so it may fail
    /// to validate at the node. In that case the application should retry again, after refreshing the ledger version.
    fn assign(&mut self, assignee: Assignment, tier: Tier, private_key: Keypair) {
        let next = self.next_free_assignment(tier);
        let assignment = TokenAssignment::assign(private_key, tier, assignee, next);
        self.append_unchecked(assignment);
    }

    fn next_free_assignment(&self, tier: Tier) -> DateTime<Utc> {
        let now = Utc::now();
        match self.tokens_by_tier.get(&tier) {
            Some(_others) => {
                //
                todo!()
            }
            None => tier.next_assignment(now),
        }
    }
}

impl TokenAssignment {
    fn assign(
        private_key: Keypair,
        tier: Tier,
        assignee: Assignment,
        time_slot: DateTime<Utc>,
    ) -> Self {
        let msg = Self::to_be_signed(&time_slot, &assignee, tier);
        let signature = private_key.sign(&msg);
        TokenAssignment {
            tier,
            time_slot,
            assignee,
            signature,
        }
    }
}
