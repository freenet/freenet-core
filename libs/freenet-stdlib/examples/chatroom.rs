#[cfg(feature = "unstable")]
mod example {

    use freenet_macros::contract;
    use freenet_stdlib::contract_composition::{ComponentParameter, ContractComponent, Mergeable};
    use freenet_stdlib::typed_contract::BincodeEncoder;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub struct ChatRoom {
        pub members: Vec<dependency_2::ChatRoomMember>,
        pub messages: Vec<dependency_1::SignedComposable<dependency_2::ChatRoomMessage>>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct ChatRoomParameters {
        pub owner_public_key: dependency_2::PublicKey,
    }

    impl ComponentParameter for ChatRoomParameters {
        fn contract_id(&self) -> Option<freenet_stdlib::prelude::ContractInstanceId> {
            unimplemented!()
        }
    }

    impl<'x> From<&'x ChatRoomParameters> for dependency_2::ChatRoomMsgParameters {
        fn from(_: &'x ChatRoomParameters) -> Self {
            unimplemented!()
        }
    }

    impl<'x> From<&'x ChatRoom> for dependency_2::PublicKey {
        fn from(_: &'x ChatRoom) -> Self {
            unimplemented!()
        }
    }

    impl<'x> From<&'x ChatRoom> for String {
        fn from(_: &'x ChatRoom) -> Self {
            unreachable!()
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct ChatRoomSummary;

    impl Mergeable<dependency_2::ChildSummary> for ChatRoomSummary {
        fn merge(&mut self, _child_summary: dependency_2::ChildSummary) {
            unreachable!()
        }
    }

    impl Mergeable<ChatRoomSummary> for ChatRoomSummary {
        fn merge(&mut self, _child_summary: ChatRoomSummary) {
            unreachable!()
        }
    }

    impl<'x> From<&'x ChatRoom> for ChatRoomSummary {
        fn from(_: &'x ChatRoom) -> Self {
            unreachable!()
        }
    }

    #[contract(
    children(
        Vec<dependency_2::ChatRoomMember>,
        Vec<dependency_1::SignedComposable<dependency_2::ChatRoomMessage>>
    ),
    encoder = BincodeEncoder
)]
    impl ContractComponent for ChatRoom {
        type Context = String;
        type Parameters = ChatRoomParameters;
        type Delta = dependency_2::ChatDelta;
        type Summary = ChatRoomSummary;

        fn verify<Child, Ctx>(
            &self,
            parameters: &Self::Parameters,
            _: &Ctx,
            related: &freenet_stdlib::prelude::RelatedContractsContainer,
        ) -> Result<freenet_stdlib::prelude::ValidateResult, freenet_stdlib::prelude::ContractError>
        where
            Child: ContractComponent,
            Self::Context: for<'x> From<&'x Ctx>,
        {
            self.messages
                .verify::<Vec<dependency_1::SignedComposable<dependency_2::ChatRoomMessage>>, _>(
                    &parameters.into(),
                    self,
                    related,
                )?;
            unimplemented!()
        }

        fn merge(
            &mut self,
            _: &Self::Parameters,
            _: &freenet_stdlib::contract_composition::TypedUpdateData<Self>,
            _: &freenet_stdlib::prelude::RelatedContractsContainer,
        ) -> freenet_stdlib::typed_contract::MergeResult {
            unimplemented!()
        }

        fn summarize<ParentSummary>(
            &self,
            _: &Self::Parameters,
            _: &mut ParentSummary,
        ) -> Result<(), freenet_stdlib::prelude::ContractError>
        where
            ParentSummary: freenet_stdlib::contract_composition::Mergeable<
                <Self as ContractComponent>::Summary,
            >,
        {
            unimplemented!()
        }

        fn delta(
            &self,
            _: &Self::Parameters,
            _: &Self::Summary,
        ) -> Result<Self::Delta, freenet_stdlib::prelude::ContractError> {
            unimplemented!()
        }
    }

    pub mod dependency_1 {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Clone)]
        pub struct Signature {}

        #[derive(Serialize, Deserialize, Clone)]
        pub struct SignedComposable<S> {
            pub value: S,
            pub signature: Signature,
        }
    }

    pub mod dependency_2 {
        use freenet_stdlib::contract_composition::{
            ComponentParameter, ContractComponent, Mergeable,
        };
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Clone)]
        pub struct ChatDelta;

        impl Mergeable<ChatDelta> for ChatDelta {
            fn merge(&mut self, _child_summary: ChatDelta) {}
        }

        impl<'x> From<&'x ChatDelta> for ChatDelta {
            fn from(_: &'x ChatDelta) -> Self {
                ChatDelta
            }
        }

        #[derive(Clone, Copy, Serialize, Deserialize)]
        pub struct ChildSummary;

        #[derive(Clone, Copy, Serialize, Deserialize)]
        pub struct PublicKey {}

        #[derive(Serialize, Deserialize, Clone)]
        pub struct ChatRoomMember {
            pub name: String,
            pub public_key: PublicKey,
        }

        #[allow(dead_code)]
        impl ContractComponent for ChatRoomMember {
            type Context = PublicKey;
            type Parameters = ChatRoomMsgParameters;
            type Delta = ChatDelta;
            type Summary = String;

            fn verify<Child, Ctx>(
                &self,
                _: &Self::Parameters,
                _: &Ctx,
                _: &freenet_stdlib::typed_contract::RelatedContractsContainer,
            ) -> Result<
                freenet_stdlib::prelude::ValidateResult,
                freenet_stdlib::prelude::ContractError,
            >
            where
                Child: ContractComponent,
                Self::Context: for<'x> From<&'x Ctx>,
            {
                unreachable!()
            }

            fn merge(
                &mut self,
                _: &Self::Parameters,
                _: &freenet_stdlib::contract_composition::TypedUpdateData<Self>,
                _: &freenet_stdlib::typed_contract::RelatedContractsContainer,
            ) -> freenet_stdlib::typed_contract::MergeResult {
                unreachable!()
            }

            fn summarize<ParentSummary>(
                &self,
                _: &Self::Parameters,
                _: &mut ParentSummary,
            ) -> Result<(), freenet_stdlib::prelude::ContractError>
            where
                ParentSummary: freenet_stdlib::contract_composition::Mergeable<
                    <Self as ContractComponent>::Summary,
                >,
            {
                unreachable!()
            }

            fn delta(
                &self,
                _: &Self::Parameters,
                _: &Self::Summary,
            ) -> Result<Self::Delta, freenet_stdlib::prelude::ContractError> {
                unreachable!()
            }
        }

        #[derive(Serialize, Deserialize, Clone)]
        pub struct ChatRoomMessage {
            pub message: String,
            pub author: String,
        }

        pub struct ChatRoomMsgParameters {
            pub owner_public_key: PublicKey,
        }

        impl ComponentParameter for ChatRoomMsgParameters {
            fn contract_id(&self) -> Option<freenet_stdlib::prelude::ContractInstanceId> {
                unimplemented!()
            }
        }

        impl ContractComponent for super::dependency_1::SignedComposable<ChatRoomMessage> {
            type Context = PublicKey;
            type Parameters = ChatRoomMsgParameters;
            type Delta = ChatDelta;
            type Summary = String;

            fn verify<Child, Ctx>(
                &self,
                parameters: &Self::Parameters,
                context: &Ctx,
                _: &freenet_stdlib::prelude::RelatedContractsContainer,
            ) -> Result<
                freenet_stdlib::prelude::ValidateResult,
                freenet_stdlib::prelude::ContractError,
            >
            where
                Child: ContractComponent,
                Self::Context: for<'x> From<&'x Ctx>,
            {
                let _public_key = parameters.owner_public_key;
                let _public_key = PublicKey::from(context);
                // do stuff with pub key
                unimplemented!()
            }

            fn merge(
                &mut self,
                _: &Self::Parameters,
                _: &freenet_stdlib::contract_composition::TypedUpdateData<Self>,
                _: &freenet_stdlib::prelude::RelatedContractsContainer,
            ) -> freenet_stdlib::typed_contract::MergeResult {
                unimplemented!()
            }

            fn summarize<ParentSummary>(
                &self,
                _: &Self::Parameters,
                _: &mut ParentSummary,
            ) -> Result<(), freenet_stdlib::prelude::ContractError>
            where
                ParentSummary: freenet_stdlib::contract_composition::Mergeable<
                    <Self as ContractComponent>::Summary,
                >,
            {
                unimplemented!()
            }

            fn delta(
                &self,
                _: &Self::Parameters,
                _: &Self::Summary,
            ) -> Result<Self::Delta, freenet_stdlib::prelude::ContractError> {
                unimplemented!()
            }
        }
    }
}

fn main() {}
