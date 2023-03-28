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
    use dioxus::prelude::UnboundedSender;
    use locutus_stdlib::client_api::{ClientError, ClientRequest, HostResponse};

    use crate::app::AsyncActionResult;

    type ClientRequester = UnboundedSender<ClientRequest<'static>>;
    type HostResponses = crossbeam::channel::Receiver<Result<HostResponse, ClientError>>;

    pub(crate) type NodeResponses = crossbeam::channel::Sender<AsyncActionResult>;

    #[cfg(feature = "use-node")]
    pub(crate) struct WebApi {
        pub host_responses: HostResponses,
        send_half: ClientRequester,
        pub client_errors: crossbeam::channel::Receiver<AsyncActionResult>,
        error_sender: NodeResponses,
        api: locutus_stdlib::client_api::WebApi,
    }

    #[cfg(not(feature = "use-node"))]
    pub(crate) struct WebApi {}

    impl WebApi {
        #[cfg(not(feature = "use-node"))]
        pub fn new() -> Result<Self, String> {
            Ok(Self {})
        }

        #[cfg(all(not(target_family = "wasm"), feature = "use-node"))]
        pub fn new() -> Result<Self, String> {
            todo!()
        }

        #[cfg(all(target_family = "wasm", feature = "use-node"))]
        pub fn new() -> Result<Self, String> {
            use futures::StreamExt;
            use wasm_bindgen::JsCast;
            let conn = web_sys::WebSocket::new("ws://localhost:50509/contract/command/").unwrap();
            let (send_host_responses, host_responses) = crossbeam::channel::unbounded();
            let (send_half, mut rx) = futures::channel::mpsc::unbounded();
            let result_handler = move |result: Result<HostResponse, ClientError>| {
                send_host_responses.send(result).expect("channel open");
            };
            let onopen_handler = || {
                web_sys::console::log_1(
                    &serde_wasm_bindgen::to_value("Connected to websocket").unwrap(),
                );
            };
            let mut api = locutus_stdlib::client_api::WebApi::start(
                conn,
                result_handler,
                |err| {
                    web_sys::console::error_1(
                        &serde_wasm_bindgen::to_value(&format!("connection error: {err}")).unwrap(),
                    );
                },
                onopen_handler,
            );
            let (error_sender, client_errors) = crossbeam::channel::unbounded();

            Ok(Self {
                host_responses,
                send_half,
                client_errors,
                error_sender,
                api,
            })
        }

        #[cfg(feature = "use-node")]
        pub fn sender_half(&self) -> WebApiRequestClient {
            WebApiRequestClient {
                sender: self.send_half.clone(),
                responses: self.error_sender.clone(),
            }
        }

        #[cfg(not(feature = "use-node"))]
        pub fn sender_half(&self) -> WebApiRequestClient {
            WebApiRequestClient
        }
    }

    #[cfg(feature = "use-node")]
    #[derive(Clone, Debug)]
    pub(crate) struct WebApiRequestClient {
        sender: ClientRequester,
        responses: NodeResponses,
    }

    #[cfg(not(feature = "use-node"))]
    #[derive(Clone, Debug)]
    pub(crate) struct WebApiRequestClient;

    impl WebApiRequestClient {
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
    impl From<WebApiRequestClient> for NodeResponses {
        fn from(val: WebApiRequestClient) -> Self {
            val.responses
        }
    }

    #[cfg(not(feature = "use-node"))]
    impl From<WebApiRequestClient> for NodeResponses {
        fn from(_val: WebApiRequestClient) -> Self {
            unimplemented!()
        }
    }
}
