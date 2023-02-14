use dioxus::prelude::*;

pub fn main() {
    #[cfg(not(target_family = "wasm"))]
    dioxus_desktop::launch(app);

    #[cfg(target_family = "wasm")]
    dioxus_web::launch(app);
}

fn app(cx: Scope) -> Element {
    cx.render(rsx! {
        div { "hello, wasm!" }
    })
}
