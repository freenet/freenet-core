#[cfg(feature = "unstable")]
mod example {

    mod parent {
        //! This would be the end crate.

        // children would be like a different dependency crate which implements composable types
        use super::children::{self, *};

        use freenet_stdlib::{contract_composition::*, prelude::*, typed_contract::MergeResult};
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize)]
        pub struct ParentContract {
            contract_b_0: ChildComponent,
            contract_b_1: ChildComponent,
        }

        #[derive(Serialize, Deserialize)]
        pub struct ParentContractParams {
            contract_b_0_params: ChildComponentParams,
            contract_b_1_params: ChildComponentParams,
        }
        impl ComponentParameter for ParentContractParams {
            fn contract_id(&self) -> Option<ContractInstanceId> {
                unimplemented!()
            }
        }

        #[derive(Serialize, Deserialize)]
        pub struct ParentContractSummary;
        impl<'a> From<&'a ParentContract> for ParentContractSummary {
            fn from(_: &'a ParentContract) -> Self {
                unimplemented!()
            }
        }
        impl<'a> From<&'a ParentContractSummary> for ChildComponentSummary {
            fn from(_: &'a ParentContractSummary) -> Self {
                unimplemented!()
            }
        }
        impl Mergeable<ChildComponentSummary> for ParentContractSummary {
            fn merge(&mut self, _: ChildComponentSummary) {
                unimplemented!()
            }
        }
        impl Mergeable<ParentContractSummary> for ParentContractSummary {
            fn merge(&mut self, _: ParentContractSummary) {
                unimplemented!()
            }
        }

        #[derive(Serialize, Deserialize)]
        pub struct ParentContractDelta {
            contract_b_0: ChildComponentDelta,
            contract_b_1: ChildComponentDelta,
        }
        impl<'a> From<&'a ParentContract> for ChildComponent {
            fn from(_: &'a ParentContract) -> Self {
                unimplemented!()
            }
        }
        impl<'a> From<&'a ParentContractDelta> for ChildComponentDelta {
            fn from(_: &'a ParentContractDelta) -> Self {
                unimplemented!()
            }
        }
        impl<'a> From<&'a ParentContractParams> for ChildComponentParams {
            fn from(_: &'a ParentContractParams) -> Self {
                unimplemented!()
            }
        }

        #[contract(children(ChildComponent, ChildComponent), encoder = BincodeEncoder)]
        // todo: this impl block would be derived ideally, we can have a derive macro
        // in the struct where the associated types need to be specified
        impl ContractComponent for ParentContract {
            type Context = NoContext;
            type Parameters = ParentContractParams;
            type Delta = ParentContractDelta;
            type Summary = ParentContractSummary;

            fn verify<Child, Ctx>(
                &self,
                parameters: &Self::Parameters,
                _ctx: &Ctx,
                related: &RelatedContractsContainer,
            ) -> Result<ValidateResult, ContractError>
            where
                Child: ContractComponent,
                Self::Context: for<'x> From<&'x Ctx>,
            {
                <ChildComponent as ContractComponent>::verify::<ChildComponent, _>(
                    &self.contract_b_0,
                    &<ChildComponent as ContractComponent>::Parameters::from(parameters),
                    self,
                    related,
                )?;
                Ok(ValidateResult::Valid)
            }

            fn merge(
                &mut self,
                parameters: &Self::Parameters,
                update_data: &TypedUpdateData<Self>,
                related: &RelatedContractsContainer,
            ) -> MergeResult {
                {
                    let sub_update: TypedUpdateData<ChildComponent> =
                        TypedUpdateData::from_other(update_data);
                    match freenet_stdlib::contract_composition::ContractComponent::merge(
                        &mut self.contract_b_0,
                        &parameters.into(),
                        &sub_update,
                        related,
                    ) {
                        MergeResult::Success => {}
                        MergeResult::RequestRelated(req) => {
                            return MergeResult::RequestRelated(req)
                        }
                        MergeResult::Error(e) => return MergeResult::Error(e),
                    }
                }
                {
                    let sub_update: TypedUpdateData<ChildComponent> =
                        TypedUpdateData::from_other(update_data);
                    match freenet_stdlib::contract_composition::ContractComponent::merge(
                        &mut self.contract_b_1,
                        &parameters.into(),
                        &sub_update,
                        related,
                    ) {
                        MergeResult::Success => {}
                        MergeResult::RequestRelated(req) => {
                            return MergeResult::RequestRelated(req)
                        }
                        MergeResult::Error(e) => return MergeResult::Error(e),
                    }
                }
                MergeResult::Success
            }

            fn summarize<ParentSummary>(
                &self,
                parameters: &Self::Parameters,
                summary: &mut ParentSummary,
            ) -> Result<(), ContractError>
            where
                ParentSummary: Mergeable<<Self as ContractComponent>::Summary>,
            {
                // todo: probably need ParentSummary to impl From<&Self>?
                let mut this_summary = ParentContractSummary;
                self.contract_b_0
                    .summarize(&parameters.into(), &mut this_summary)?;
                self.contract_b_1
                    .summarize(&parameters.into(), &mut this_summary)?;
                summary.merge(this_summary);
                Ok(())
            }

            fn delta(
                &self,
                parameters: &Self::Parameters,
                summary: &Self::Summary,
            ) -> Result<Self::Delta, ContractError> {
                // todo: this impl may be probematic to derive, specially getting the return type
                // maybe requires adding an other transformation bound
                let contract_b_0 = self
                    .contract_b_0
                    .delta(&parameters.into(), &summary.into())?;
                let contract_b_1 = self
                    .contract_b_0
                    .delta(&parameters.into(), &summary.into())?;
                Ok(ParentContractDelta {
                    contract_b_0,
                    contract_b_1,
                })
            }
        }

        impl<'x> From<&'x ParentContract> for children::PubKey {
            fn from(_: &'x ParentContract) -> Self {
                children::PubKey
            }
        }
    }

    mod children {
        //! This would be a depebdency crate.

        use freenet_stdlib::{
            contract_composition::*,
            prelude::*,
            typed_contract::{MergeResult, Related, TypedContract},
        };
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize)]
        pub struct ChildComponent {}

        #[derive(Serialize, Deserialize, Clone)]
        pub struct ChildComponentParams {
            other_params: CParams,
        }

        impl ComponentParameter for ChildComponentParams {
            fn contract_id(&self) -> Option<ContractInstanceId> {
                unimplemented!()
            }
        }

        #[derive(Serialize, Deserialize)]
        pub struct ChildComponentSummary;

        #[derive(Serialize, Deserialize)]
        pub struct ChildComponentDelta;

        pub struct PubKey;

        impl From<ChildComponentParams> for PubKey {
            fn from(_: ChildComponentParams) -> Self {
                PubKey
            }
        }

        impl ContractComponent for ChildComponent {
            type Context = PubKey;
            type Summary = ChildComponentSummary;
            type Parameters = ChildComponentParams;
            type Delta = ChildComponentDelta;

            fn verify<Child, Ctx>(
                &self,
                _parameters: &Self::Parameters,
                context: &Ctx,
                _related: &RelatedContractsContainer,
            ) -> Result<ValidateResult, ContractError>
            where
                Child: ContractComponent,
                Self::Context: for<'x> From<&'x Ctx>,
            {
                let _pub_key = PubKey::from(context);
                // assert something in self/context is signed with pub key
                Ok(ValidateResult::Valid)
            }

            fn merge(
                &mut self,
                parameters: &Self::Parameters,
                _delta: &TypedUpdateData<Self>,
                related: &RelatedContractsContainer,
            ) -> MergeResult {
                let contract_id = parameters.contract_id().unwrap();
                let _other_contract = match related.get::<Contract>(&parameters.other_params) {
                    Ok(r) => {
                        let Related::Found { state, .. } = r else {
                            let mut req = RelatedContractsContainer::default();
                            req.request::<Contract>(contract_id);
                            return MergeResult::RequestRelated(req);
                        };
                        state
                    }
                    Err(e) => return MergeResult::Error(e.into()),
                };
                MergeResult::Success
            }

            fn delta(
                &self,
                _parameters: &Self::Parameters,
                _summary: &Self::Summary,
            ) -> Result<Self::Delta, ContractError> {
                Ok(ChildComponentDelta)
            }

            fn summarize<ParentSummary>(
                &self,
                _parameters: &Self::Parameters,
                summary: &mut ParentSummary,
            ) -> Result<(), ContractError>
            where
                ParentSummary: Mergeable<<Self as ContractComponent>::Summary>,
            {
                summary.merge(ChildComponentSummary);
                Ok(())
            }
        }

        #[derive(Serialize, Deserialize)]
        pub struct Contract {}
        #[derive(Serialize, Deserialize, Clone, Copy)]
        pub struct CParams;
        #[derive(Serialize, Deserialize)]
        pub struct CDelta;
        #[derive(Serialize, Deserialize)]
        pub struct CSummary;

        use freenet_stdlib::prelude::EncodingAdapter;
        impl EncodingAdapter for Contract {
            type Parameters = CParams;
            type Delta = CDelta;
            type Summary = CSummary;
            type SelfEncoder = BincodeEncoder<Self>;
            type ParametersEncoder = BincodeEncoder<Self::Parameters>;
            type DeltaEncoder = BincodeEncoder<Self::Delta>;
            type SummaryEncoder = BincodeEncoder<Self::Summary>;
        }

        impl TypedContract for Contract {
            fn verify(
                &self,
                _: Self::Parameters,
                _: RelatedContractsContainer,
            ) -> Result<ValidateResult, ContractError> {
                unimplemented!()
            }

            fn merge(
                &mut self,
                _: &Self::Parameters,
                _: encoding::TypedUpdateData<Self>,
                _: &RelatedContractsContainer,
            ) -> MergeResult {
                unimplemented!()
            }

            fn summarize(&self, _: Self::Parameters) -> Result<Self::Summary, ContractError> {
                unimplemented!()
            }

            fn delta(
                &self,
                _: Self::Parameters,
                _: Self::Summary,
            ) -> Result<Self::Delta, ContractError> {
                unimplemented!()
            }

            fn instance_id(_: &Self::Parameters) -> ContractInstanceId {
                unimplemented!()
            }
        }
    }
}

fn main() {}
