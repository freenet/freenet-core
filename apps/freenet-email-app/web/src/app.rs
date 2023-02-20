#![allow(non_snake_case)]
use std::{borrow::Cow, cell::RefCell, rc::Rc};

use dioxus::prelude::*;

use crate::MAIN_ELEMENT_ID;

#[derive(Debug, Clone)]
struct Inbox {
    emails: RefCell<Vec<Email>>,
}

impl Inbox {}

#[derive(Debug, Clone)]
struct Email {
    id: u64,
    sender: Cow<'static, str>,
    title: Cow<'static, str>,
    content: Cow<'static, str>,
}

impl Inbox {
    fn new() -> Self {
        let emails = if cfg!(feature = "ui-testing") {
            vec![
                Email {
                    id: 0,
                    sender: "mary".into(),
                    title: "1st msg".into(),
                    content: "".into(),
                },
                Email {
                    id: 1,
                    sender: "jane".to_string().into(),
                    title: "2nd msg".to_string().into(),
                    content: "".into(),
                },
            ]
        } else {
            todo!()
        };
        Self {
            emails: RefCell::new(emails),
        }
    }
}

struct User {
    logged: bool,
}

pub(crate) fn App(cx: Scope) -> Element {
    // todo: create a connection to the node ws api and retrieve the current inbox state
    use_shared_state_provider(cx, || User { logged: false });

    let logged_state = use_shared_state::<User>(cx).unwrap();
    if logged_state.read().logged {
        use_context_provider(cx, Inbox::new);
        cx.render(rsx! {
           Inbox {}
        })
    } else {
        cx.render(rsx! {
           LoggingScreen {}
        })
    }
}

struct NewMessageWindow {
    show: bool,
}

fn Inbox(cx: Scope) -> Element {
    use_shared_state_provider(cx, || NewMessageWindow { show: false });

    cx.render(rsx!(
        div {
            class: "columns",
            nav {
                class: "column is-one-fifth menu",
                UserMenu {}
            }
            div {
                class: "column",
                InboxList {}
                NewMessageWindow {}
            }
        }
    ))
}

fn UserMenu(cx: Scope) -> Element {
    let new_msg_window = use_shared_state::<NewMessageWindow>(cx).unwrap();
    let user = use_shared_state::<User>(cx).unwrap();
    cx.render(rsx!(
        div {
            class: "pl-3 pr-3 mt-3",
            ul {
                class: "menu-list",
                li {
                    a {
                        class: "is-active",
                        "Received"
                    }
                }
                li {
                    a {
                        onclick: move |_| {
                            let mut new_msg_state = new_msg_window.write();
                            if new_msg_state.show {
                                new_msg_state.show = false;
                            } else {
                                new_msg_state.show = true;
                            }
                        },
                        "Write message"
                    }
                }
                li {
                    a {
                        onclick: move |_| {
                            let mut logged_state = user.write();
                            logged_state.logged = false;
                        },
                        "Log out"
                    }
                }
            }
        }
    ))
}

fn InboxList(cx: Scope) -> Element {
    let inbox = use_context::<Inbox>(cx).unwrap();

    #[inline_props]
    fn EmailLink<'a>(cx: Scope<'a>, sender: Cow<'a, str>, title: Cow<'a, str>) -> Element {
        cx.render(rsx!(tr {
            "sender: {sender}; title: {title}"
        }))
    }

    if !inbox.emails.borrow().is_empty() {
        let emails = inbox.emails.borrow();
        let links = emails.iter().map(|email| {
            rsx!(EmailLink {
                sender: email.sender.clone(),
                title: email.title.clone()
            })
        });
        cx.render(rsx! {
            table {
                links,
            }
        })
    } else {
        None
    }
}

fn LoggingScreen(cx: Scope) -> Element {
    let logged_state = use_shared_state::<User>(cx).unwrap();
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
                    div { class: "field",
                        label { "Address" }
                        div {
                            class: "control has-icons-left",
                            input { class: "input", placeholder: "Address" }
                            span { class: "icon is-small is-left", i { class: "fas fa-envelope" } }
                        }
                    }
                    div { class: "field",
                        label { "Key" }
                        div {
                            class: "control has-icons-left",
                            input { class: "input", placeholder: "Key" }
                            span { class: "icon is-small is-left", i { class: "fas fa-key" } }
                        }
                    }
                    a {
                        class: "is-link",
                        onclick: move |_| {},
                        "Login "
                    }
                }
                div {
                    class: "box is-small",
                    div {
                        class: "container ",
                        a {
                            class: "is-link",
                            onclick: move |_| {
                                let mut logged_state = logged_state.write();
                                logged_state.logged = true;
                            },
                            "Create new account"
                        }
                    }
                }
            }
        }
    })
}

fn NewMessageWindow(cx: Scope) -> Element {
    let state = use_shared_state::<NewMessageWindow>(cx).unwrap();
    let state = state.read();
    if state.show {
        cx.render(rsx! {
            div {
                input {
                    "New message..."
                }
            }
        })
    } else {
        None
    }
}
