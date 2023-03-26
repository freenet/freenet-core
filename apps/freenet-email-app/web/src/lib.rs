mod app;
pub(crate) mod inbox;
#[cfg(test)]
pub(crate) mod test_util;

const MAIN_ELEMENT_ID: &str = "freenet-email-main";

type DynError = Box<dyn std::error::Error + Send + Sync>;

pub fn main() {
    #[cfg(not(target_family = "wasm"))]
    {
        use tracing_subscriber::{filter::LevelFilter, EnvFilter};
        let _e = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .from_env_lossy(),
            )
            .try_init();
        use dioxus_desktop::{tao::dpi::LogicalPosition, LogicalSize};
        use dioxus_desktop::{Config, WindowBuilder};
        const INDEX: &str = r#"
            <!DOCTYPE html>
            <html>
            <head>
                <title>Freenet Email</title>
                <meta name="viewport" content="width=device-width, initial-scale=1.0" />
                <!-- CUSTOM HEAD -->
                <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css">
                <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.3.0/css/all.min.css" integrity="sha512-SzlrxWUlpfuzQ+pcUCosxcglQRNAq/DZjVsC0lE40xsADsfeQoEypE+enwcOiGjk/bSuGGKHEyjSoQ1zVisanQ==" crossorigin="anonymous" referrerpolicy="no-referrer" />        
            </head>
            <body>
                <div id="freenet-email-main" class="container"></div>
                <!-- MODULE LOADER -->
            </body>
            </html>
        "#;

        dioxus_desktop::launch_cfg(
            app::App,
            Config::new()
                .with_root_name(MAIN_ELEMENT_ID)
                .with_custom_index(INDEX.to_string())
                .with_resource_directory(env!("CARGO_MANIFEST_DIR"))
                .with_window(
                    WindowBuilder::new()
                        .with_title("Freenet Email App")
                        .with_inner_size(LogicalSize::new(1200, 800))
                        .with_position(LogicalPosition::new(200, 100)),
                ),
        );
    }

    #[cfg(target_family = "wasm")]
    {
        use dioxus_web::Config;
        dioxus_web::launch_cfg(app::App, Config::new().rootname(MAIN_ELEMENT_ID));
    }
}

mod api {
    use dioxus::prelude::{Scope, UnboundedSender};
    use locutus_stdlib::client_api::{ClientError, ClientRequest, HostResponse};

    use crate::app::AsyncActionResult;

    type SenderHalf = UnboundedSender<ClientRequest<'static>>;
    type ReceiverHalf = crossbeam::channel::Receiver<Result<HostResponse, ClientError>>;

    pub(crate) type NodeResponses = crossbeam::channel::Sender<AsyncActionResult>;

    #[cfg(feature = "use-node")]
    pub(crate) struct WebApi {
        pub receiver_half: ReceiverHalf,
        sender_half: SenderHalf,
        pub responses: crossbeam::channel::Receiver<AsyncActionResult>,
        responses_sender: NodeResponses,
    }

    #[cfg(not(feature = "use-node"))]
    pub(crate) struct WebApi {}

    #[cfg(feature = "use-node")]
    #[derive(Clone)]
    pub(crate) struct WebApiSender {
        sender: SenderHalf,
        responses: NodeResponses,
    }

    #[cfg(not(feature = "use-node"))]
    #[derive(Clone)]
    pub(crate) struct WebApiSender;

    impl WebApiSender {
        #[cfg(feature = "use-node")]
        pub async fn send(
            &mut self,
            request: locutus_stdlib::client_api::ClientRequest<'static>,
        ) -> Result<(), locutus_stdlib::client_api::Error> {
            use futures::SinkExt;

            self.sender
                .send(request)
                .await
                .map_err(|_| locutus_stdlib::client_api::Error::ChannelClosed)
        }

        #[cfg(not(feature = "use-node"))]
        pub async fn send(
            &mut self,
            request: locutus_stdlib::client_api::ClientRequest<'static>,
        ) -> Result<(), locutus_stdlib::client_api::Error> {
            tracing::debug!(?request, "emulated request");
            Ok(())
        }
    }

    #[cfg(feature = "use-node")]
    impl From<WebApiSender> for NodeResponses {
        fn from(val: WebApiSender) -> Self {
            val.responses
        }
    }

    #[cfg(not(feature = "use-node"))]
    impl From<WebApiSender> for NodeResponses {
        fn from(_val: WebApiSender) -> Self {
            unimplemented!()
        }
    }

    impl WebApi {
        #[cfg(not(feature = "use-node"))]
        pub fn new(_: Scope) -> Result<Self, String> {
            Ok(Self {})
        }

        #[cfg(all(not(target_family = "wasm"), feature = "use-node"))]
        pub fn new(_: Scope) -> Result<Self, String> {
            todo!()
        }

        #[cfg(all(target_family = "wasm", feature = "use-node"))]
        pub fn new(cx: Scope) -> Result<Self, String> {
            use futures::StreamExt;
            let conn = web_sys::WebSocket::new("ws://localhost:50509/contract/command/").unwrap();
            let (tx, receiver_half) = crossbeam::channel::unbounded();
            let (sender_half, mut rx) = futures::channel::mpsc::unbounded();
            let result_handler = move |result: Result<HostResponse, ClientError>| {
                tx.send(result).expect("channel open");
            };
            let mut api = locutus_stdlib::client_api::WebApi::start(
                conn,
                result_handler,
                |err| {
                    web_sys::console::error_1(
                        &serde_wasm_bindgen::to_value(&format!("connection error: {err}")).unwrap(),
                    );
                },
                || {},
            );
            cx.spawn({
                async move {
                    while let Some(msg) = rx.next().await {
                        api.send(msg)
                            .await
                            .map_err(|e| {
                                tracing::error!("{e}");
                                e
                            })
                            .unwrap();
                    }
                }
            });
            let (responses_sender, responses) = crossbeam::channel::unbounded();
            Ok(Self {
                receiver_half,
                sender_half,
                responses,
                responses_sender,
            })
        }

        #[cfg(feature = "use-node")]
        pub fn sender_half(&self) -> WebApiSender {
            WebApiSender {
                sender: self.sender_half.clone(),
                responses: self.responses_sender.clone(),
            }
        }

        #[cfg(not(feature = "use-node"))]
        pub fn sender_half(&self) -> WebApiSender {
            WebApiSender
        }
    }
}
