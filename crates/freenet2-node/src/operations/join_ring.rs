use rust_fsm::*;

use crate::{
    conn_manager::ConnectionBridge2,
    message::{JoinRing, Message, Transaction},
    node::OpStateStorage,
};

use super::{OpError, OperationResult};

state_machine! {
    derive(Debug)
    pub(crate) JoinRingOp(Connecting)

    Connecting =>  {
        Connecting => OCReceived [OCReceived],
        OCReceived => Connected [Connected],
        Connected => Connected [Connected],
    },
    OCReceived(Connected) => Connected [Connected],
}

pub(crate) async fn join_ring<CB>(
    op_storage: &mut OpStateStorage,
    conn_manager: &mut CB,
    join_op: JoinRing,
) -> Result<(), OpError>
where
    CB: ConnectionBridge2,
{
    let id = *join_op.id();
    if let Some(state) = op_storage.pop_join_ring_op(&id) {
        // was an existing operation
        match update_state(state, join_op) {
            Err(tx) => {
                log::error!("error while processing {}", tx);
                conn_manager.send(Message::Canceled(id)).await?;
            }
            Ok(OperationResult {
                return_msg: Some(msg),
                state: Some(updated_state),
            }) => {
                conn_manager.send(msg).await?;
                op_storage.push_join_ring_op(id, updated_state)?;
            }
            Ok(OperationResult {
                return_msg: Some(msg),
                state: None,
            }) => {
                // finished the operation at this node, informing back
                conn_manager.send(msg).await?;
            }
            Ok(OperationResult {
                return_msg: None,
                state: None,
            }) => {
                // operation finished_completely
            }
            _ => unreachable!(),
        }
    } else {
    }
    Ok(())
}

fn update_state(
    current_state: JoinRingOp,
    other_host_msg: JoinRing,
) -> Result<OperationResult<JoinRingOp>, Transaction> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_fsm::StateMachine;

    #[test]
    fn join_ring_transitions() {
        let mut join_op_host_1 = StateMachine::<JoinRingOp>::new();
        let res = join_op_host_1
            .consume(&JoinRingOpInput::Connecting)
            .unwrap()
            .unwrap();
        assert!(matches!(res, JoinRingOpOutput::OCReceived));

        let mut join_op_host_2 = StateMachine::<JoinRingOp>::new();
        let res = join_op_host_2
            .consume(&JoinRingOpInput::OCReceived)
            .unwrap()
            .unwrap();
        assert!(matches!(res, JoinRingOpOutput::Connected));

        let res = join_op_host_1
            .consume(&JoinRingOpInput::Connected)
            .unwrap()
            .unwrap();
        assert!(matches!(res, JoinRingOpOutput::Connected));
    }
}
