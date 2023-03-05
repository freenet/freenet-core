#![allow(non_snake_case)]
use std::{borrow::Cow, cell::RefCell};

use dioxus::prelude::*;
use ed25519_dalek::Keypair;

mod login;

#[derive(Debug, Clone)]
struct Inbox {
    emails: RefCell<Vec<Email>>,
}

impl Inbox {}

impl Inbox {
    fn new() -> Self {
        let emails = if cfg!(feature = "ui-testing") {
            vec![
                Email {
                    id: 0,
                    sender: "Mary".into(),
                    title: "Unread email from Mary".into(),
                    content: "".into(),
                    read: false,
                },
                Email {
                    id: 1,
                    sender: "Jane".to_string().into(),
                    title: "Email from Jane".to_string().into(),
                    content: "".into(),
                    read: true,
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

#[derive(Default)]
struct User {
    logged: bool,
    peer: Option<Keypair>,
    password: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Eq)]
struct Email {
    id: u64,
    sender: Cow<'static, str>,
    title: Cow<'static, str>,
    content: Cow<'static, str>,
    read: bool,
}

impl PartialEq for Email {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PartialOrd for Email {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Email {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub(crate) fn App(cx: Scope) -> Element {
    // todo: create a connection to the node ws api and retrieve the current inbox state
    use_shared_state_provider(cx, || User {
        logged: true,
        ..Default::default()
    });

    let user = use_shared_state::<User>(cx).unwrap();
    if user.read().logged {
        use_context_provider(cx, Inbox::new);
        cx.render(rsx! {
           UserInbox {}
        })
    } else {
        cx.render(rsx! {
           login::LoggingScreen {}
        })
    }
}

struct NewMessageWindow {
    show: bool,
}

fn UserInbox(cx: Scope) -> Element {
    use_shared_state_provider(cx, || NewMessageWindow { show: false });
    cx.render(rsx!(
        div {
            class: "columns",
            nav {
                class: "column is-one-fifth menu",
                UserMenuComponent {}
            }
            div {
                class: "column",
                InboxComponent {}
            }
        }
    ))
}

fn UserMenuComponent(cx: Scope) -> Element {
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

fn InboxComponent(cx: Scope) -> Element {
    let inbox = use_context::<Inbox>(cx).unwrap();
    use_shared_state_provider(cx, OpenEmail::default);

    #[derive(Default)]
    struct OpenEmail(Option<u64>);

    #[inline_props]
    fn EmailLink<'a>(
        cx: Scope<'a>,
        sender: Cow<'a, str>,
        title: Cow<'a, str>,
        read: bool,
        id: u64,
    ) -> Element {
        let open_mail = use_shared_state::<OpenEmail>(cx).unwrap();
        let icon_style = read
            .then(|| "fa-regular fa-envelope")
            .unwrap_or("fa-solid fa-envelope");
        cx.render(rsx!(a {
            class: "panel-block",
            id: "email-inbox-accessor-{id}",
            onclick: move |_| { open_mail.write().0 = Some(*id); },
            span {
                class: "panel-icon",
                i { class: icon_style }
            }
            span { class: "ml-2", "{sender}" }
            span { class: "ml-5", "{title}" }
        }))
    }

    let emails = inbox.emails.borrow();

    let opened_email = use_shared_state::<OpenEmail>(cx).unwrap();
    let opened_email = opened_email.read();
    if let Some(email_id) = opened_email.0 {
        let id_p = (*emails).binary_search_by_key(&email_id, |e| e.id).unwrap();
        let email = &emails[id_p];
        cx.render(rsx! {
            div {
                class: "panel is-link mt-3",
                p { class: "panel-heading", "Inbox" }
                div {
                    class: "panel-block",
                    p {
                        class: "control has-icons-left",
                        input { class: "input is-link", r#type: "text", placeholder: "Search" }
                        span { class: "icon is-left", i { class: "fas fa-search", aria_hidden: true } }
                    }
                }
            }
        })
    } else {
        let links = emails.iter().map(|email| {
            rsx!(EmailLink {
                sender: email.sender.clone(),
                title: email.title.clone()
                read: email.read,
                id: email.id,
            })
        });
        cx.render(rsx! {
            div {
                class: "panel is-link mt-3",
                p { class: "panel-heading", "Inbox" }
                p {
                    class: "panel-tabs",
                    a {
                        class: "is-active icon-text",
                        span { class: "icon", i { class: "fas fa-thin fa-inbox" } }
                        span { "Primary" }
                    }
                    a {
                        class: "icon-text",
                        span { class: "icon",i { class: "fas fa-thin fa-user-group" } },
                        span { "Social" }
                    }
                    a {
                        class: "icon-text",
                        span { class: "icon", i { class: "fas fa-thin fa-circle-exclamation" } },
                        span { "Updates" }
                    }
                }
                div {
                    class: "panel-block",
                    p {
                        class: "control has-icons-left",
                        input { class: "input is-link", r#type: "text", placeholder: "Search" }
                        span { class: "icon is-left", i { class: "fas fa-search", aria_hidden: true } }
                    }
                }
                links
            }
        })
    }
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
