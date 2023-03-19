use dioxus::prelude::*;

use crate::app::User;

const DEFAULT_ID_ICON: &str = "data:image/x-icon;base64,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";

struct ImportId(bool);

fn LoginHeader(cx: Scope) -> Element {
    cx.render(rsx! {
        div {
            class: "columns",
            div { class: "column is-4" }
            section {
                class: "section is-small",
                h1 { class: "title", "Freenet Email" }
                h2 { class: "subtitle", "Nice " strong { "caption " } "text" }
            }
        }
    })
}

pub(super) fn IdentifiersList(cx: Scope) -> Element {
    let user = use_shared_state::<User>(cx).unwrap();
    cx.render(rsx! {
        LoginHeader {}
        div {
            class: "columns",
            div { class: "column is-3" }
            div {
                class: "column is-6",
                div {
                    class: "card has-background-light is-small mt-2",
                    div {
                        class: "card-content",
                        div {
                            class: "media",
                            div {
                                class: "media-left",
                                figure { class: "image is-48x48", img { src: DEFAULT_ID_ICON } }
                            }
                            div {
                                class: "media-content",
                                p {
                                    class: "title is-4",
                                    a {
                                        style: "color: inherit",
                                        onclick: move |_| user.write().set_logged_id(0),
                                        "Ian Clarke"
                                    }
                                },
                                p { class: "subtitle is-6", "ian.clarke@freenet.org" }
                            }
                        }
                    },
                    div {
                        class: "card-content",
                        div {
                            class: "media",
                            div {
                                class: "media-left",
                                figure { class: "image is-48x48", img { src: DEFAULT_ID_ICON } }
                            }
                            div {
                                class: "media-content",
                                p {
                                    class: "title is-4",
                                    a {
                                        style: "color: inherit",
                                        onclick: move |_| user.write().set_logged_id(1),
                                        "Ian's Other Account"
                                    }
                                },
                                p { class: "subtitle is-6", "other.stuff@freenet.org" }
                            }
                        }
                    },
                    div {
                        class: "card-content columns",
                        div { class: "column is-4" }
                        a {
                            class: "column is-4 is-link",
                            "Create new identity"
                        }
                    }
                }
            }
        }
    })
}

pub(super) fn GetOrCreateIndentity(cx: Scope) -> Element {
    use_shared_state_provider::<ImportId>(cx, || ImportId(false));
    let import_form = use_shared_state::<ImportId>(cx).unwrap();
    cx.render(rsx! {
        LoginHeader {}
        div {
            class: "columns",
            div { class: "column is-4"},
            div {
                class: "column is-4",
                if !import_form.read().0 {
                    CreateLinks(cx)
                } else {
                    ImportForm(cx)
                }
            }
        }
    })
}

fn CreateLinks(cx: Scope) -> Element {
    let user = use_shared_state::<User>(cx).unwrap();
    let create_user_form = use_shared_state::<ImportId>(cx).unwrap();
    cx.render(rsx! {
        div {
            class: "box is-small",
            a {
                class: "is-link",
                onclick: move |_| {
                    create_user_form.write().0 = false;
                    user.write().identified = true;
                },
                "Create new identity"
            }
        },
        div {
            class: "box is-small",
            a {
                class: "is-link",
                onclick: move |_| create_user_form.write().0 = true ,
                "Import existing identity"
            }
        }
    })
}

fn ImportForm(cx: Scope) -> Element {
    let user = use_shared_state::<User>(cx).unwrap();
    let create_id_form = use_shared_state::<ImportId>(cx).unwrap();
    let address = use_state(cx, String::new);
    let key = use_state(cx, String::new);
    let key_path = use_state(cx, || {
        std::iter::repeat('\u{80}')
            .take(100)
            .chain(std::iter::repeat('.').take(300))
            .collect::<String>()
    });

    cx.render(rsx! {
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
                    span { class: "file-label", "Or import key file" }
                }
                span { class: "file-name has-background-white", "{key_path}" }
                }
            }
            a {
                class: "is-link",
                onclick: move |_|  {
                    create_id_form.write().0 = false;
                    user.write().identified = true;
                },
                "Sign up"
            }
        }
    })
}
