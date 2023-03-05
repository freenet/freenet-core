use dioxus::prelude::*;

use crate::app::User;

pub(super) fn LoggingScreen(cx: Scope) -> Element {
    let user = use_shared_state::<User>(cx).unwrap();
    let address = use_state(cx, String::new);
    let key = use_state(cx, String::new);
    let key_path = use_state(cx, || {
        std::iter::repeat('\u{80}')
            .take(100)
            .chain(std::iter::repeat('.').take(300))
            .collect::<String>()
    });
    let send = use_state(cx, || false);

    use_shared_state_provider(cx, || CreateUser(false));
    let create_user_form = use_shared_state::<CreateUser>(cx).unwrap();

    let login_info_length = address.get().len() > 3 && key.get().len() > 3;
    if login_info_length && *send.get() {
        // todo: verify
        user.write().logged = true;
    }

    let create_form_show = create_user_form.read().0;
    let create_div_style = create_form_show
        .then(|| "box is-small has-background-light")
        .unwrap_or("box is-small");

    cx.render(rsx! {
        div {
        class: "columns",
        div { class: "column is-4" }
        section {
            class: "section is-medium",
            h1 { class: "title", "Freenet Email" }
            h2 { class: "subtitle", "Nice " strong { "caption " } "text" }
        }
        }
        div {
        class: "columns",
        div { class: "column is-4" }
        div {
            class: "column is-4",
            div {
            class: "box has-background-primary is-small mt-2",
            div {
                class: "field",
                label { "Address" }
                div {
                class: "control has-icons-left",
                input {
                    class: "input",
                    placeholder: "Address",
                    value: "{address}",
                    oninput: move |evt| address.set(evt.value.clone())
                }
                span { class: "icon is-small is-left", i { class: "fas fa-envelope" } }
                }
            }
            div {
                class: "field",
                label { "Key" }
                div {
                class: "control has-icons-left",
                input {
                    class: "input",
                    placeholder: "Key",
                    value: "{key}",
                    oninput: move |evt| key.set(evt.value.clone())
                }
                span { class: "icon is-small is-left", i { class: "fas fa-key" } }
                }
            }
            div {
                class: "file is-small has-name mb-2 mt-2",
                label {
                class: "file-label",
                input { class: "file-input", r#type: "file", name: "keypair-file" }
                span {
                    class: "file-cta",
                    span { class: "file-icon", i { class: "fas fa-upload" } }
                    span { class: "file-label", "Key file" }
                }
                span { class: "file-name has-background-white", "{key_path}" }
                }
            }
            a {
                class: "is-link",
                onclick: move |_| {
                if login_info_length {
                    send.set(true)
                }
                },
                "Sign in"
            }
            },
            div {
            class: create_div_style,
            if !create_form_show {
                CreateLink(cx)
            } else {
                CreateForm(cx)
            }
            }
        }
        }
    })
}

struct CreateUser(bool);

fn CreateLink(cx: Scope) -> Element {
    let _user = use_shared_state::<User>(cx).unwrap();
    let create_user_form = use_shared_state::<CreateUser>(cx).unwrap();
    cx.render(rsx! {
    div {
        class: "container ",
        a {
        class: "is-link",
        onclick: move |_| create_user_form.write().0 = true ,
        "Create new account"
        }
    }
    })
}

fn CreateForm(cx: Scope) -> Element {
    let user = use_shared_state::<User>(cx).unwrap();
    let create_user_form = use_shared_state::<CreateUser>(cx).unwrap();
    let address = use_state(cx, String::new);
    let key = use_state(cx, String::new);

    cx.render(rsx! {
    div {
        class: "container",
        div { class: "field",
        label { "Address" }
        div {
            class: "control has-icons-left",
            input {
            class: "input",
            placeholder: "Address",
            value: "{address}",
            oninput: move |evt| address.set(evt.value.clone())
            }
            span { class: "icon is-small is-left", i { class: "fas fa-envelope" } }
        }
        },
        div { class: "field",
        label { "Key" }
        div {
            class: "control has-icons-left",
            input {
            class: "input",
            placeholder: "Key",
            value: "{key}",
            oninput: move |evt| key.set(evt.value.clone())
            }
            span { class: "icon is-small is-left", i { class: "fas fa-key" } }
        }
        },
        a {
        class: "is-link",
        onclick: move |_|  {
            create_user_form.write().0 = false;
            user.write().logged = true;
        },
        "Sign up"
        }
    }
    })
}
