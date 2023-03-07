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
                    content: "Lorem ipsum dolor sit amet, consectetur adipiscing elit..."
                        .repeat(10)
                        .into(),
                    read: false,
                },
                Email {
                    id: 1,
                    sender: "Jane".to_string().into(),
                    title: "Email from Jane".to_string().into(),
                    content: "Lorem ipsum dolor sit amet, consectetur adipiscing elit..."
                        .repeat(10)
                        .into(),
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

#[derive(Debug, Clone, Eq, Props)]
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

#[derive(Default)]
struct MenuSelection {
    email: Option<u64>,
    new_msg: bool,
}

impl MenuSelection {
    fn set_new_msg(&mut self) {
        if self.new_msg {
            self.new_msg = false;
        } else {
            self.new_msg = true;
            self.email = None;
        }
    }

    fn new_msg(&self) -> bool {
        self.new_msg
    }

    fn set_received(&mut self) {
        self.email = None;
        self.new_msg = false;
    }

    fn received(&self) -> bool {
        !self.new_msg && self.email.is_none()
    }
}

fn UserInbox(cx: Scope) -> Element {
    use_shared_state_provider(cx, MenuSelection::default);
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
    let user = use_shared_state::<User>(cx).unwrap();
    let menu_selection = use_shared_state::<MenuSelection>(cx).unwrap();

    let received_class = (menu_selection.read().received() || !menu_selection.read().new_msg())
        .then(|| "is-active")
        .unwrap_or("");
    let write_msg_class = menu_selection
        .read()
        .new_msg()
        .then(|| "is-active")
        .unwrap_or("");

    cx.render(rsx!(
        div {
            class: "pl-3 pr-3 mt-3",
            ul {
                class: "menu-list",
                li {
                    a {
                        class: received_class,
                        onclick: move |_| { menu_selection.write().set_received(); },
                        "Received"
                    }
                }
                li {
                    a {
                        class: write_msg_class,
                        onclick: move |_| {
                            let mut selection = menu_selection.write();
                            selection.set_new_msg();
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
    let menu_selection = use_shared_state::<MenuSelection>(cx).unwrap();

    #[inline_props]
    fn EmailLink<'a>(
        cx: Scope<'a>,
        sender: Cow<'a, str>,
        title: Cow<'a, str>,
        read: bool,
        id: u64,
    ) -> Element {
        let open_mail = use_shared_state::<MenuSelection>(cx).unwrap();
        let icon_style = read
            .then(|| "fa-regular fa-envelope")
            .unwrap_or("fa-solid fa-envelope");
        cx.render(rsx!(a {
            class: "panel-block",
            id: "email-inbox-accessor-{id}",
            onclick: move |_| { open_mail.write().email = Some(*id); },
            span {
                class: "panel-icon",
                i { class: icon_style }
            }
            span { class: "ml-2", "{sender}" }
            span { class: "ml-5", "{title}" }
        }))
    }

    let emails = inbox.emails.borrow();

    let menu_selection_ref = menu_selection.read();
    let new_msg = menu_selection_ref.new_msg;
    if let Some(email_id) = menu_selection_ref.email {
        let id_p = (*emails).binary_search_by_key(&email_id, |e| e.id).unwrap();
        let email = &emails[id_p];
        cx.render(rsx! {
            OpenMessage {
                id: email.id,
                sender: email.sender.clone(),
                title: email.title.clone(),
                content: email.content.clone(),
                read: email.read,
            }
        })
    } else if new_msg {
        cx.render(rsx! {
            NewMessageWindow {}
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
                        span { class: "icon", i { class: "fas fa-inbox" } }
                        span { "Primary" }
                    }
                    a {
                        class: "icon-text",
                        span { class: "icon",i { class: "fas fa-user-group" } },
                        span { "Social" }
                    }
                    a {
                        class: "icon-text",
                        span { class: "icon", i { class: "fas fa-circle-exclamation" } },
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

fn OpenMessage(cx: Scope<Email>) -> Element {
    let menu_selection = use_shared_state::<MenuSelection>(cx).unwrap();
    let email = cx.props;
    cx.render(rsx! {
        div {
            class: "columns title mt-3",
            div {
                class: "column",
                a {
                    class: "icon is-small",
                    onclick: move |_| { menu_selection.write().email = None; },
                    i { class: "fa-sharp fa-solid fa-arrow-left", aria_label: "Back to Inbox", style: "color:#4a4a4a" }, 
                }
            }
            div { class: "column is-four-fifths", h2 { "{email.title}" } }
            div {
                class: "column", 
                a { class: "icon is-small", 
                onclick: move |_| { menu_selection.write().email = None; },
                i { class: "fa-sharp fa-solid fa-trash", aria_label: "Delete", style: "color:#4a4a4a" } } 
            }
        }
        div {
            id: "email-content-{email.id}",
            p {
                "{email.content}"
            }
        }
    })
}

fn NewMessageWindow(cx: Scope) -> Element {
    cx.render(rsx! {
        div {
            class: "column mt-3",
            div {
                class: "box has-background-light",
                h3 { class: "title is-3", "New message" }
                table {
                    class: "table is-narrow has-background-light",
                    tbody {
                        tr {
                            th { "From" }
                            td { style: "width: 100%", contenteditable: true, br {} }
                        }
                        tr {
                            th { "To"}
                            td { style: "width: 100%", contenteditable: true, br {} }
                        }
                        tr {
                            th { "Title"}
                            td { style: "width: 100%", contenteditable: true, br {} }
                        }
                    }
                }
            }
            div {
                class: "box",
                div { contenteditable: true, br {} }
            }
            div {
                button { class: "button is-info is-outlined", "Send" }
            }
        }
    })
}
