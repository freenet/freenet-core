use freenet_stdlib::client_api::ClientRequest;

use crate::CommandReceiver;

use super::{state::AppState, ExecutorConfig};

pub(super) async fn wasm_runtime(
    _config: ExecutorConfig,
    mut command_receiver: CommandReceiver,
    mut app: AppState,
) -> Result<(), anyhow::Error> {
    loop {
        let req = command_receiver.recv().await;
        let dc = execute_command(
            req.ok_or_else(|| anyhow::anyhow!("channel closed"))?,
            &mut app,
        )
        .await?;
        if dc {
            break;
        }
    }
    Ok(())
}

async fn execute_command(
    req: ClientRequest<'static>,
    app: &mut AppState,
) -> Result<bool, anyhow::Error> {
    let node = &mut *app.local_node.write().await;
    match req {
        ClientRequest::ContractOp(_) => {
            node.send(req).await?;
            Ok(false)
        }
        _ => {
            tracing::error!("op not supported");
            anyhow::bail!("op not support");
        }
    }
}
