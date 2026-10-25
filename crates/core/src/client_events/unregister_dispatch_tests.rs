//! `UnregisterDelegate` through the real client dispatch: `process_open_request`
//! into the PRODUCTION contract handler (`NetworkContractHandler` over a real
//! `RuntimePool`) running a real WASM delegate.
//!
//! The executor's own refusal is pinned in
//! `contract::executor::runtime::unregister_tests`. These tests drive the layer
//! above it, which clears a delegate's push registrations: a guard in the
//! executor says nothing about what this layer did before reaching it.

use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::client_api::{
    ClientRequest, DelegateError, DelegateRequest, ErrorKind, HostResponse, RequestError,
};
use freenet_stdlib::prelude::*;
use tokio::sync::mpsc;

use super::{ClientId, ConnectionScope, Error, HostResult, OpenRequest, process_open_request};
use crate::config::{ConfigArgs, GlobalExecutor};
use crate::contract::OperationMode;
use crate::contract::delegate_app_registry;
use crate::contract::{ContractHandler, NetworkContractHandler, contract_handling};
use crate::node::OpManager;
use crate::wasm_runtime::UserSecretContext;

/// A node whose `OpManager` feeds the production contract handler loop, so a
/// request sent through `process_open_request` is executed for real.
struct Node {
    op_manager: Arc<OpManager>,
    delegates_dir: std::path::PathBuf,
    handle: tokio::task::JoinHandle<Result<(), crate::contract::ContractError>>,
    _guards: Box<dyn std::any::Any>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Node {
    async fn start(id: &str) -> Self {
        Self::start_with(id, false).await
    }

    async fn start_with(id: &str, hosted_mode: bool) -> Self {
        let config_args = ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(OperationMode::Local),
            ws_api: crate::config::WebsocketApiArgs {
                hosted_mode: Some(hosted_mode),
                ..Default::default()
            },
            ..Default::default()
        };
        let node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        let config = node_config.config.clone();
        let delegates_dir = config.delegates_dir();

        let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, handler_half, wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, result_router_rx) = mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let op_manager = Arc::new(
            OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);

        // The loop reads the half the OpManager sends to, so this is the path
        // a client request takes in a running node.
        let handler = NetworkContractHandler::build(handler_half, op_manager.clone(), config)
            .await
            .expect("build the production contract handler");
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));
        Node {
            op_manager,
            delegates_dir,
            handle,
            _guards: Box::new((
                notification_rx,
                wait_for_event,
                result_router_rx,
                task_monitor,
            )),
        }
    }

    /// Send one delegate request the way a client connection does.
    async fn send(
        &self,
        client_id: ClientId,
        req: DelegateRequest<'static>,
        scope: ConnectionScope,
        notification: Option<mpsc::Sender<HostResult>>,
    ) -> Result<(), Error> {
        self.send_as(client_id, req, scope, notification, None)
            .await
    }

    /// [`Self::send`] from a hosted user's connection.
    async fn send_as(
        &self,
        client_id: ClientId,
        req: DelegateRequest<'static>,
        scope: ConnectionScope,
        notification: Option<mpsc::Sender<HostResult>>,
        user: Option<UserSecretContext>,
    ) -> Result<(), Error> {
        let mut open = OpenRequest::new(client_id, Box::new(ClientRequest::DelegateOp(req)))
            .with_connection_scope(scope)
            .with_user_context(user);
        if let Some(tx) = notification {
            open = open.with_notification(tx);
        }
        let fut = process_open_request(open, self.op_manager.clone(), None).await;
        tokio::time::timeout(Duration::from_secs(60), fut)
            .await
            .expect("the request is answered")
            .map(drop)
    }
}

/// A registered real delegate, an app registered for its pushes over a local
/// connection, and the app's channel.
struct Registered {
    key: DelegateKey,
    params: Vec<u8>,
    app: ClientId,
    app_rx: mpsc::Receiver<HostResult>,
}

async fn register_delegate_and_app(node: &Node, params: &[u8]) -> Registered {
    register_delegate_and_app_as(node, params, None).await
}

/// [`register_delegate_and_app`], with the app registered over `user`'s
/// connection. On a hosted node every delegate run belongs to a user, so the
/// app's request there must carry one.
async fn register_delegate_and_app_as(
    node: &Node,
    params: &[u8],
    user: Option<&UserSecretContext>,
) -> Registered {
    let code = tokio::task::spawn_blocking(|| {
        crate::wasm_runtime::tests::get_test_module("test_delegate_echo_params")
            .expect("build the fixture delegate")
    })
    .await
    .expect("join fixture build");
    let delegate = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
        &code.into(),
        &params.to_vec().into(),
    ))));
    let key = delegate.key().clone();
    node.send(
        ClientId::next(),
        DelegateRequest::RegisterDelegate {
            delegate,
            cipher: [0u8; 32],
            nonce: [0u8; 24],
        },
        ConnectionScope::Local,
        None,
    )
    .await
    .expect("local registration");

    // An ApplicationMessages request carrying a notification channel is what
    // registers an app for the delegate's pushes.
    let app = ClientId::next();
    let (tx, mut app_rx) = mpsc::channel(16);
    node.send_as(
        app,
        ask(&key, params),
        ConnectionScope::Local,
        Some(tx),
        user.cloned(),
    )
    .await
    .expect("the delegate answers");
    // That request's own reply arrives on the channel; drain it.
    let reply = app_rx.recv().await.expect("the reply").expect("a success");
    assert!(
        matches!(reply, HostResponse::DelegateResponse { .. }),
        "got {reply:?}"
    );
    assert_eq!(delegate_app_registry::registration_count(&key), 1);
    Registered {
        key,
        params: params.to_vec(),
        app,
        app_rx,
    }
}

fn ask(key: &DelegateKey, params: &[u8]) -> DelegateRequest<'static> {
    DelegateRequest::ApplicationMessages {
        key: key.clone(),
        params: Parameters::from(params.to_vec()),
        inbound: vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(b"params?".to_vec()),
        )],
    }
}

/// The app still receives the delegate's pushes.
fn assert_push_delivered(r: &mut Registered, after: &str) {
    assert_eq!(
        delegate_app_registry::route_to_apps(&r.key, Ok(HostResponse::Ok)),
        1,
        "{after}: the app's push registration must still deliver"
    );
    let pushed = r.app_rx.try_recv().expect("the push reached the app");
    assert!(matches!(pushed, Ok(HostResponse::Ok)), "got {pushed:?}");
}

/// A non-local `UnregisterDelegate` sent through the client dispatch is
/// refused with the typed error the client receives, and afterwards the
/// delegate still runs and its app's push registration still delivers. The
/// same request from a local client unregisters and clears the registration.
///
/// FALSIFY by moving `delegate_app_registry::remove_delegate` in
/// `process_open_request` above the dispatch: the registration assertion
/// fails. FALSIFY by removing the executor's scope check: the request is no
/// longer refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_non_local_unregister_through_dispatch_keeps_the_delegate_and_its_pushes() {
    let node = Node::start("unregister-dispatch-remote").await;
    let mut r = register_delegate_and_app(&node, b"unregister-dispatch-remote").await;

    let err = node
        .send(
            ClientId::next(),
            DelegateRequest::UnregisterDelegate(r.key.clone()),
            ConnectionScope::Remote,
            None,
        )
        .await
        .expect_err("a non-local unregister must be refused");
    let Error::Executor(exec_err) = &err else {
        panic!("expected an executor error, got {err:?}");
    };
    assert!(exec_err.is_unregister_not_local(), "got {err}");
    // What the client receives: a typed delegate error naming the rule.
    let client_err =
        super::typed_delegate_client_error(err).expect("the refusal must reach the client typed");
    let ErrorKind::RequestError(RequestError::DelegateError(DelegateError::ExecutionError(msg))) =
        client_err.kind()
    else {
        panic!("expected DelegateError::ExecutionError, got {client_err:?}");
    };
    assert!(
        msg.starts_with(crate::wasm_runtime::DELEGATE_UNREGISTER_NOT_LOCAL),
        "got {msg}"
    );
    assert!(super::is_routine_delegate_client_error(&client_err));

    assert_eq!(
        delegate_app_registry::registration_count(&r.key),
        1,
        "a refused unregister must leave the app's registration"
    );
    assert_push_delivered(&mut r, "after a refused remote unregister");
    node.send(
        ClientId::next(),
        ask(&r.key, &r.params),
        ConnectionScope::Local,
        None,
    )
    .await
    .expect("the delegate must still be registered and runnable");

    // Control: a local unregister removes the delegate and the registration.
    node.send(
        ClientId::next(),
        DelegateRequest::UnregisterDelegate(r.key.clone()),
        ConnectionScope::Local,
        None,
    )
    .await
    .expect("a local unregister succeeds");
    assert_eq!(
        delegate_app_registry::registration_count(&r.key),
        0,
        "a local unregister clears the app's registration"
    );
    let gone = node
        .send(
            ClientId::next(),
            ask(&r.key, &r.params),
            ConnectionScope::Local,
            None,
        )
        .await
        .expect_err("the delegate is gone");
    let Error::Executor(gone) = gone else {
        panic!("expected an executor error, got {gone:?}");
    };
    assert!(gone.is_missing_delegate(), "got {gone}");
    delegate_app_registry::remove_client(r.app);
}

/// When the store cannot remove the delegate, the client is told the
/// unregister failed, and the app keeps its push registration because the
/// delegate is still registered.
///
/// The failure is injected by putting a directory where the delegate's `.reg`
/// file was.
///
/// FALSIFY by clearing the registration before the executor answers, or by
/// answering `Ok` on a failed store removal: the registration assertion fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_unregister_through_dispatch_keeps_the_pushes() {
    let node = Node::start("unregister-dispatch-fails").await;
    let mut r = register_delegate_and_app(&node, b"unregister-dispatch-fails").await;

    let reg = node
        .delegates_dir
        .join(r.key.encode())
        .with_extension("reg");
    std::fs::remove_file(&reg).expect("the registration wrote a .reg file");
    std::fs::create_dir(&reg).expect("plant a directory in its place");

    let err = node
        .send(
            ClientId::next(),
            DelegateRequest::UnregisterDelegate(r.key.clone()),
            ConnectionScope::Local,
            None,
        )
        .await
        .expect_err("a failed unregister must be reported");
    let Error::Executor(exec_err) = &err else {
        panic!("expected an executor error, got {err:?}");
    };
    assert!(!exec_err.is_delegate_refusal(), "got {err}");
    assert_eq!(
        delegate_app_registry::registration_count(&r.key),
        1,
        "a failed unregister must leave the app's registration"
    );
    assert_push_delivered(&mut r, "after a failed unregister");
    delegate_app_registry::remove_client(r.app);
}

/// A hosted user's app registration for `key`, made the way a hosted
/// connection makes one, with the channel its pushes arrive on.
async fn register_app_as(
    node: &Node,
    key: &DelegateKey,
    params: &[u8],
    user: &UserSecretContext,
) -> (ClientId, mpsc::Receiver<HostResult>) {
    let client = ClientId::next();
    let (tx, mut rx) = mpsc::channel(16);
    node.send_as(
        client,
        ask(key, params),
        ConnectionScope::Local,
        Some(tx),
        Some(user.clone()),
    )
    .await
    .expect("the delegate answers");
    let reply = rx.recv().await.expect("the reply").expect("a success");
    assert!(
        matches!(reply, HostResponse::DelegateResponse { .. }),
        "got {reply:?}"
    );
    (client, rx)
}

/// On a hosted node, user A's `UnregisterDelegate` removes only A's own push
/// registration. User B, who uses the same delegate, keeps theirs, still
/// receives the delegate's pushes, and can still run the delegate.
///
/// FALSIFY by clearing every registration for the key after an `Ok`
/// (`remove_delegate` instead of `remove_user_registrations` in
/// `process_open_request`): the registration count assertion fails. FALSIFY by removing the
/// executor's `user_context` branch: the hosted node refuses A's request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hosted_users_unregister_leaves_other_users_registration_and_delegate() {
    const PARAMS: &[u8] = b"unregister-dispatch-hosted-users";
    let node = Node::start_with("unregister-dispatch-hosted-users", true).await;
    // A third user's app, registered with the delegate, which must survive
    // A's unregister too.
    let c = UserSecretContext::from_token(b"unregister-dispatch-user-c");
    let mut r = register_delegate_and_app_as(&node, PARAMS, Some(&c)).await;
    let a = UserSecretContext::from_token(b"unregister-dispatch-user-a");
    let b = UserSecretContext::from_token(b"unregister-dispatch-user-b");
    let (a_client, _a_rx) = register_app_as(&node, &r.key, PARAMS, &a).await;
    let (b_client, mut b_rx) = register_app_as(&node, &r.key, PARAMS, &b).await;
    assert_eq!(delegate_app_registry::registration_count(&r.key), 3);

    node.send_as(
        a_client,
        DelegateRequest::UnregisterDelegate(r.key.clone()),
        ConnectionScope::Local,
        None,
        Some(a.clone()),
    )
    .await
    .expect("a hosted user may unregister for themselves");

    assert_eq!(
        delegate_app_registry::registration_count(&r.key),
        2,
        "only the unregistering user's registration goes"
    );
    assert_eq!(
        delegate_app_registry::route_to_apps(&r.key, Ok(HostResponse::Ok)),
        2,
        "the other two users still receive pushes"
    );
    let pushed = b_rx.try_recv().expect("the push reached the other user");
    assert!(matches!(pushed, Ok(HostResponse::Ok)), "got {pushed:?}");
    let pushed = r
        .app_rx
        .try_recv()
        .expect("the push reached the third user");
    assert!(matches!(pushed, Ok(HostResponse::Ok)), "got {pushed:?}");
    node.send_as(
        ClientId::next(),
        ask(&r.key, PARAMS),
        ConnectionScope::Local,
        None,
        Some(b.clone()),
    )
    .await
    .expect("the delegate is still registered for the other user");

    delegate_app_registry::remove_client(r.app);
    delegate_app_registry::remove_client(a_client);
    delegate_app_registry::remove_client(b_client);
}

/// On a hosted node, a client with no user token is refused with the typed
/// error, and every registration and the delegate survive.
///
/// FALSIFY by removing the executor's `hosted_mode` check: the request is no
/// longer refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tokenless_hosted_client_is_refused_through_dispatch() {
    const PARAMS: &[u8] = b"unregister-dispatch-hosted-tokenless";
    let node = Node::start_with("unregister-dispatch-hosted-tokenless", true).await;
    let user = UserSecretContext::from_token(b"unregister-dispatch-tokenless-owner");
    let mut r = register_delegate_and_app_as(&node, PARAMS, Some(&user)).await;

    let err = node
        .send(
            ClientId::next(),
            DelegateRequest::UnregisterDelegate(r.key.clone()),
            ConnectionScope::Local,
            None,
        )
        .await
        .expect_err("a tokenless hosted client must be refused");
    let Error::Executor(exec_err) = &err else {
        panic!("expected an executor error, got {err:?}");
    };
    assert!(exec_err.is_unregister_hosted(), "got {err}");
    let client_err =
        super::typed_delegate_client_error(err).expect("the refusal must reach the client typed");
    let ErrorKind::RequestError(RequestError::DelegateError(DelegateError::ExecutionError(msg))) =
        client_err.kind()
    else {
        panic!("expected DelegateError::ExecutionError, got {client_err:?}");
    };
    assert!(
        msg.starts_with(crate::wasm_runtime::DELEGATE_UNREGISTER_HOSTED),
        "got {msg}"
    );
    assert!(super::is_routine_delegate_client_error(&client_err));

    assert_eq!(delegate_app_registry::registration_count(&r.key), 1);
    assert_push_delivered(&mut r, "after a refused tokenless hosted unregister");
    node.send_as(
        ClientId::next(),
        ask(&r.key, &r.params),
        ConnectionScope::Local,
        None,
        Some(user.clone()),
    )
    .await
    .expect("the delegate must still be registered");
    delegate_app_registry::remove_client(r.app);
}

/// On a hosted node every delegate run belongs to a user, so an
/// `ApplicationMessages` request with no user is refused through the client
/// dispatch with the typed refusal, reaches the client typed, is routine (not
/// logged as an error), registers nothing for pushes, and leaves the delegate
/// running for a user.
///
/// FALSIFY by removing the hosted check from the executor's
/// `ApplicationMessages` arm: the request succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hosted_run_without_a_user_is_refused_through_dispatch() {
    const PARAMS: &[u8] = b"dispatch-hosted-run-without-user";
    let node = Node::start_with("dispatch-hosted-run-without-user", true).await;
    let user = UserSecretContext::from_token(b"dispatch-hosted-run-owner");
    let r = register_delegate_and_app_as(&node, PARAMS, Some(&user)).await;

    let err = node
        .send(
            ClientId::next(),
            ask(&r.key, PARAMS),
            ConnectionScope::Local,
            None,
        )
        .await
        .expect_err("a hosted node must refuse a run with no user");
    let Error::Executor(exec_err) = &err else {
        panic!("expected an executor error, got {err:?}");
    };
    assert!(exec_err.is_hosted_run_without_user(), "got {err}");
    let client_err =
        super::typed_delegate_client_error(err).expect("the refusal must reach the client typed");
    let ErrorKind::RequestError(RequestError::DelegateError(DelegateError::ExecutionError(msg))) =
        client_err.kind()
    else {
        panic!("expected DelegateError::ExecutionError, got {client_err:?}");
    };
    assert!(
        msg.starts_with(crate::wasm_runtime::DELEGATE_HOSTED_RUN_WITHOUT_USER),
        "got {msg}"
    );
    assert!(super::is_routine_delegate_client_error(&client_err));

    node.send_as(
        ClientId::next(),
        ask(&r.key, PARAMS),
        ConnectionScope::Local,
        None,
        Some(user.clone()),
    )
    .await
    .expect("the same request with a user runs");
    delegate_app_registry::remove_client(r.app);
}
