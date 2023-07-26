extern crate core;

pub(crate) mod aft;
mod api;
mod app;
pub(crate) mod inbox;
pub(crate) mod log;
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
            app::app,
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
        dioxus_web::launch_cfg(app::app, Config::new().rootname(MAIN_ELEMENT_ID));
    }
}
