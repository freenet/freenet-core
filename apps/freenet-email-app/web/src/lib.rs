use locutus_stdlib::client_api::{ClientError, HostResponse};

mod app;
pub(crate) mod inbox;


pub fn main() {
    #[cfg(not(target_family = "wasm"))]
    {
        const MAIN_ELEMENT_ID: &str = "freenet-email-main";
        use dioxus_desktop::{tao::dpi::LogicalPosition, LogicalSize};
        use dioxus_desktop::{Config, WindowBuilder};
        // if cfg!(debug_assertions) {
        //     hot_reload_init!();
        // }
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
    dioxus_web::launch(app::App);
}

struct WebApi {
    api: locutus_stdlib::client_api::WebApi,
    received: crossbeam::channel::Receiver<Result<HostResponse, ClientError>>,
}

impl WebApi {
    #[cfg(target_family = "wasm")]
    fn new() -> Result<Self, String> {
        let conn = web_sys::WebSocket::new("ws://localhost:55008/contract/command").unwrap();
        let (tx, received) = crossbeam::channel::unbounded();
        let result_handler = move |result: Result<HostResponse, ClientError>| {
            tx.send(result).expect("channel open");
        };
        let api = locutus_stdlib::client_api::WebApi::start(
            conn,
            result_handler,
            |err| {
                web_sys::console::error_1(
                    &serde_wasm_bindgen::to_value(&format!("connection error: {err}")).unwrap(),
                );
            },
            || {},
        );
        Ok(Self { api, received })
    }

    async fn send(
        &mut self,
        request: locutus_stdlib::client_api::ClientRequest<'static>,
    ) -> Result<(), locutus_stdlib::client_api::Error> {
        self.api.send(request).await
    }

    async fn recv(&mut self) -> Result<HostResponse, ClientError> {
        self.received.recv().expect("channel open")
    }
}
