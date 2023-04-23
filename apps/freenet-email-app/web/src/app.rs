#![allow(non_snake_case)]
use std::sync::Arc;
use std::{borrow::Cow, cell::RefCell, rc::Rc};

use arc_swap::ArcSwap;
use chrono::Utc;
use dioxus::prelude::*;
use futures::future::LocalBoxFuture;
use futures::{FutureExt, SinkExt};
use locutus_stdlib::prelude::ContractKey;
use once_cell::sync::{Lazy, OnceCell};
use rsa::pkcs1::EncodeRsaPublicKey;
use rsa::{
    pkcs1::DecodeRsaPrivateKey, pkcs1::DecodeRsaPublicKey, pkcs8::LineEnding, RsaPrivateKey,
    RsaPublicKey,
};
use std::collections::HashMap;

use crate::{
    api::{NodeResponses, WebApiRequestClient},
    inbox::{DecryptedMessage, InboxModel, MessageModel},
    DynError,
};

mod login;

pub(crate) type AsyncActionResult = Result<(), (DynError, TryNodeAction)>;

static ALIAS_MAP: Lazy<HashMap<String, String>> = Lazy::new(|| {
    const RSA_PRIV_0_PEM: &str = include_str!("../examples/rsa4096-id-0-priv.pem");
    const RSA_PRIV_1_PEM: &str = include_str!("../examples/rsa4096-id-1-priv.pem");
    let pub_key0: String = RsaPrivateKey::from_pkcs1_pem(RSA_PRIV_0_PEM)
        .unwrap()
        .to_public_key()
        .to_pkcs1_pem(LineEnding::LF)
        .unwrap();
    let pub_key1: String = RsaPrivateKey::from_pkcs1_pem(RSA_PRIV_1_PEM)
        .unwrap()
        .to_public_key()
        .to_pkcs1_pem(LineEnding::LF)
        .unwrap();
    let mut map = HashMap::new();
    map.insert("address1".to_string(), pub_key0);
    map.insert("address2".to_string(), pub_key1);
    map
});

#[derive(Clone, Debug)]
pub(crate) enum NodeAction {
    LoadMessages(Identity),
}

#[derive(Clone, Debug)]
pub(crate) enum TryNodeAction {
    LoadInbox,
    SendMessage,
    RemoveMessages,
    GetAlias,
}

impl std::fmt::Display for TryNodeAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryNodeAction::LoadInbox => write!(f, "load messages"),
            TryNodeAction::SendMessage => write!(f, "send message"),
            TryNodeAction::RemoveMessages => write!(f, "remove messages"),
            TryNodeAction::GetAlias => write!(f, "get alias"),
        }
    }
}

pub(crate) static WEB_API_SENDER: OnceCell<WebApiRequestClient> = OnceCell::new();

pub(crate) fn App(cx: Scope) -> Element {
    crate::log::log("render app");
    use_shared_state_provider(cx, User::new);
    let user = use_shared_state::<User>(cx).unwrap();

    use_context_provider(cx, Inbox::new);
    let inbox = use_context::<Inbox>(cx).unwrap();
    let inbox_data = inbox.inbox_data.clone();

    #[cfg(feature = "use-node")]
    {
        let _sync = use_coroutine::<NodeAction, _, _>(cx, move |rx| {
            crate::api::node_comms(rx, user.read().identities.clone(), inbox_data)
        });
    }

    if !user.read().identified {
        cx.render(rsx! {
            login::GetOrCreateIndentity {}
        })
    } else if let Some(id) = user.read().logged_id() {
        #[cfg(feature = "use-node")]
        {
            inbox.load_messages(cx, id).expect("load messages");
        }
        #[cfg(all(feature = "ui-testing", not(feature = "use-node")))]
        {
            inbox.load_messages(id).unwrap();
        }
        cx.render(rsx! {
           UserInbox {}
        })
    } else {
        cx.render(rsx! {
           login::IdentifiersList {}
        })
    }
}

pub(crate) type InboxesData = Arc<ArcSwap<Vec<Rc<RefCell<InboxModel>>>>>;

#[derive(Debug, Clone)]
pub struct Inbox {
    inbox_data: InboxesData,
    /// loaded messages for the currently selected `active_id`
    messages: Rc<RefCell<Vec<Message>>>,
    active_id: usize,
}

impl Inbox {
    fn new() -> Self {
        Self {
            inbox_data: Arc::new(ArcSwap::from_pointee(vec![])),
            messages: Rc::new(RefCell::new(vec![])),
            active_id: 0,
        }
    }

    pub(crate) async fn load_all(
        client: WebApiRequestClient,
        contracts: &[Identity],
        contract_to_id: &mut HashMap<ContractKey, Identity>,
    ) {
        for identity in contracts {
            let mut client = client.clone();
            let res = InboxModel::load(&mut client, identity).await.map(|key| {
                contract_to_id
                    .entry(key.clone())
                    .or_insert(identity.clone());
                key
            });
            error_handling(client.into(), res.map(|_| ()), TryNodeAction::LoadInbox).await;
        }
    }

    fn send_message(
        &self,
        client: WebApiRequestClient,
        from: &str,
        to: &str,
        title: &str,
        content: &str,
    ) -> Result<Vec<LocalBoxFuture<'static, ()>>, DynError> {
        tracing::debug!("sending message from {from}");
        let content = DecryptedMessage {
            title: title.to_owned(),
            content: content.to_owned(),
            from: "".to_owned(),
            to: vec![to.to_owned()],
            cc: vec![],
            time: Utc::now(),
        };
        let mut futs = Vec::with_capacity(content.to.len());
        #[cfg(feature = "use-node")]
        {
            crate::log::log(format!("sending message from {from}"));
            for k in content.to.iter() {
                let key = RsaPublicKey::from_pkcs1_pem(k).map_err(|e| format!("{e}"))?;
                let content = content.clone();
                let mut client = client.clone();
                let f = async move {
                    let res = InboxModel::send_message(&mut client, content, key).await;
                    error_handling(client.into(), res, TryNodeAction::SendMessage).await;
                };
                futs.push(f.boxed_local());
            }
        }
        let _ = client;
        Ok(futs)
    }

    fn remove_messages(
        &self,
        client: WebApiRequestClient,
        ids: &[u64],
    ) -> Result<LocalBoxFuture<'static, ()>, DynError> {
        tracing::debug!("removing messages: {ids:?}");
        let inbox_data = self.inbox_data.load();
        let mut inbox = inbox_data[self.active_id].borrow_mut();
        inbox.remove_messages(client, ids)
    }

    // Remove the messages from the inbox contract, and move them to local storage
    fn mark_as_read(
        &self,
        client: WebApiRequestClient,
        ids: &[u64],
    ) -> Result<LocalBoxFuture<'static, ()>, DynError> {
        let messages = &mut *self.messages.borrow_mut();
        let mut removed_messages = Vec::with_capacity(ids.len());
        for e in messages {
            if ids.contains(&e.id) {
                e.read = true;
                let m = e.clone();
                removed_messages.push(m);
            }
        }
        // todo: persist in a sidekick `removed_messages`
        self.remove_messages(client, ids)
    }

    #[cfg(all(feature = "ui-testing", not(feature = "use-node")))]
    fn load_messages(&self, id: &Identity) -> Result<(), DynError> {
        let emails = {
            if id.id == 0 {
                vec![
                    Message {
                        id: 0,
                        from: "Ian's Other Account".into(),
                        title: "Email from Ian's Other Account".into(),
                        content: "Lorem ipsum dolor sit amet, consectetur adipiscing elit..."
                            .repeat(10)
                            .into(),
                        read: false,
                    },
                    Message {
                        id: 1,
                        from: "Mary".to_string().into(),
                        title: "Email from Mary".to_string().into(),
                        content: "Lorem ipsum dolor sit amet, consectetur adipiscing elit..."
                            .repeat(10)
                            .into(),
                        read: false,
                    },
                ]
            } else {
                vec![
                    Message {
                        id: 0,
                        from: "Ian Clarke".into(),
                        title: "Email from Ian".into(),
                        content: "Lorem ipsum dolor sit amet, consectetur adipiscing elit..."
                            .repeat(10)
                            .into(),
                        read: false,
                    },
                    Message {
                        id: 1,
                        from: "Jane".to_string().into(),
                        title: "Email from Jane".to_string().into(),
                        content: "Lorem ipsum dolor sit amet, consectetur adipiscing elit..."
                            .repeat(10)
                            .into(),
                        read: false,
                    },
                ]
            }
        };
        self.messages.replace(emails);
        Ok(())
    }

    #[cfg(feature = "use-node")]
    fn load_messages(&self, cx: Scope, id: &Identity) -> Result<(), DynError> {
        let actions = use_coroutine_handle::<NodeAction>(cx).unwrap();
        actions.send(NodeAction::LoadMessages(id.clone()));
        Ok(())
    }

    // #[cfg(feature = "use-node")]
    fn get_public_key_from_alias(&self, alias: &str) -> Result<String, DynError> {
        crate::log::log(format!("getting user from alias: {}", alias));
        let pub_key = ALIAS_MAP.get(alias).ok_or("alias not found")?;
        Ok(pub_key.to_string())
    }
}

struct User {
    logged: bool,
    identified: bool,
    active_id: Option<usize>,
    identities: Vec<Identity>,
}

impl User {
    // todo: enable feature gates after impl the other `use-node` version
    // #[cfg(feature = "ui-testing")]
    fn new() -> Self {
        const RSA_PRIV_0_PEM: &str = include_str!("../examples/rsa4096-id-0-priv.pem");
        const RSA_PRIV_1_PEM: &str = include_str!("../examples/rsa4096-id-1-priv.pem");
        let key0 = RsaPrivateKey::from_pkcs1_pem(RSA_PRIV_0_PEM).unwrap();
        let key1 = RsaPrivateKey::from_pkcs1_pem(RSA_PRIV_1_PEM).unwrap();
        let identified = true;
        User {
            logged: false,
            identified,
            active_id: None,
            identities: vec![
                Identity {
                    alias: "address1".to_owned(),
                    id: 0,
                    key: key0,
                },
                Identity {
                    alias: "address2".to_owned(),
                    id: 1,
                    key: key1,
                },
            ],
        }
    }

    // #[cfg(all(not(feature = "ui-testing"), feature = "use-node"))]
    // fn new() -> Self {
    //     // TODO: here we should load the user identities from the identity component
    //     todo!()
    // }

    fn logged_id(&self) -> Option<&Identity> {
        self.active_id.and_then(|id| self.identities.get(id))
    }

    fn set_logged_id(&mut self, id: usize) {
        assert!(id < self.identities.len());
        self.active_id = Some(id);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Identity {
    pub id: usize,
    pub key: RsaPrivateKey,
    alias: String,
}

#[derive(Debug, Clone, Eq, Props)]
struct Message {
    id: u64,
    from: Cow<'static, str>,
    title: Cow<'static, str>,
    content: Cow<'static, str>,
    read: bool,
}

impl From<MessageModel> for Message {
    fn from(value: MessageModel) -> Self {
        Message {
            id: value.id,
            from: value.content.from.into(),
            title: value.content.title.into(),
            content: value.content.content.into(),
            read: false,
        }
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PartialOrd for Message {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

mod menu {
    #[derive(Default)]
    pub(super) struct MenuSelection {
        email: Option<u64>,
        new_msg: bool,
    }

    impl MenuSelection {
        pub fn at_new_msg(&mut self) {
            if self.new_msg {
                self.new_msg = false;
            } else {
                self.new_msg = true;
                self.email = None;
            }
        }

        pub fn is_new_msg(&self) -> bool {
            self.new_msg
        }

        pub fn at_inbox_list(&mut self) {
            self.email = None;
            self.new_msg = false;
        }

        pub fn is_received(&self) -> bool {
            !self.new_msg && self.email.is_none()
        }

        pub fn open_email(&mut self, id: u64) {
            self.email = Some(id);
        }

        pub fn email(&self) -> Option<u64> {
            self.email
        }
    }
}

fn UserInbox(cx: Scope) -> Element {
    use_shared_state_provider(cx, menu::MenuSelection::default);
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
    let menu_selection = use_shared_state::<menu::MenuSelection>(cx).unwrap();

    let received_class = (menu_selection.read().is_received()
        || !menu_selection.read().is_new_msg())
    .then(|| "is-active")
    .unwrap_or("");
    let write_msg_class = menu_selection
        .read()
        .is_new_msg()
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
                        onclick: move |_| { menu_selection.write().at_inbox_list(); },
                        "Received"
                    }
                }
                li {
                    a {
                        class: write_msg_class,
                        onclick: move |_| {
                            let mut selection = menu_selection.write();
                            selection.at_new_msg();
                        },
                        "Write message"
                    }
                }
                li {
                    a {
                        onclick: move |_| {
                            let mut logged_state = user.write();
                            logged_state.logged = false;
                            logged_state.active_id = None;
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
    let menu_selection = use_shared_state::<menu::MenuSelection>(cx).unwrap();

    #[inline_props]
    fn EmailLink<'a>(
        cx: Scope<'a>,
        sender: Cow<'a, str>,
        title: Cow<'a, str>,
        read: bool,
        id: u64,
    ) -> Element {
        let open_mail = use_shared_state::<menu::MenuSelection>(cx).unwrap();
        let icon_style = read
            .then(|| "fa-regular fa-envelope")
            .unwrap_or("fa-solid fa-envelope");
        cx.render(rsx!(a {
            class: "panel-block",
            id: "email-inbox-accessor-{id}",
            onclick: move |_| { open_mail.write().open_email(*id); },
            span {
                class: "panel-icon",
                i { class: icon_style }
            }
            span { class: "ml-2", "{sender}" }
            span { class: "ml-5", "{title}" }
        }))
    }

    let emails = inbox.messages.borrow();
    let is_email = menu_selection.read().email();
    if let Some(email_id) = is_email {
        let id_p = (*emails).binary_search_by_key(&email_id, |e| e.id).unwrap();
        let email = &emails[id_p];
        cx.render(rsx! {
            OpenMessage {
                id: email.id,
                from: email.from.clone(),
                title: email.title.clone(),
                content: email.content.clone(),
                read: email.read,
            }
        })
    } else if menu_selection.read().is_new_msg() {
        cx.render(rsx! {
            NewMessageWindow {}
        })
    } else {
        let links = emails.iter().map(|email| {
            rsx!(EmailLink {
                sender: email.from.clone(),
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

fn OpenMessage(cx: Scope<Message>) -> Element {
    let menu_selection = use_shared_state::<menu::MenuSelection>(cx).unwrap();
    let client = WEB_API_SENDER.get().unwrap();
    let inbox = use_context::<Inbox>(cx).unwrap();
    let email = cx.props;
    let email_id = [cx.props.id];

    let result = inbox.mark_as_read(client.clone(), &email_id).unwrap();
    cx.spawn(result);

    let delete = move |_| {
        let result = inbox.remove_messages(client.clone(), &email_id).unwrap();
        cx.spawn(result);
        menu_selection.write().at_inbox_list();
    };

    cx.render(rsx! {
        div {
            class: "columns title mt-3",
            div {
                class: "column",
                a {
                    class: "icon is-small",
                    onclick: move |_| {
                        menu_selection.write().at_inbox_list();
                    },
                    i { class: "fa-sharp fa-solid fa-arrow-left", aria_label: "Back to Inbox", style: "color:#4a4a4a" }, 
                }
            }
            div { class: "column is-four-fifths", h2 { "{email.title}" } }
            div {
                class: "column", 
                a {
                    class: "icon is-small", 
                    onclick: delete,
                    i { class: "fa-sharp fa-solid fa-trash", aria_label: "Delete", style: "color:#4a4a4a" } 
                }
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
    let menu_selection = use_shared_state::<menu::MenuSelection>(cx).unwrap();
    let client = WEB_API_SENDER.get().unwrap();
    let inbox = use_context::<Inbox>(cx).unwrap();
    let user = use_shared_state::<User>(cx).unwrap();
    let user = user.read();
    let user_alias = user.logged_id().unwrap().alias.as_str();
    let to = use_state(cx, String::new);
    let title = use_state(cx, String::new);
    let content = use_state(cx, String::new);

    let alias = user_alias.to_string();
    let send_msg = move |_| {
        let receiver_public_key = match inbox.get_public_key_from_alias(to.get()) {
            Ok(v) => v,
            Err(e) => {
                crate::log::error(format!("{e}"), Some(TryNodeAction::GetAlias));
                return;
            }
        };
        match inbox.send_message(
            client.clone(),
            &alias,
            receiver_public_key.as_str(),
            title.get(),
            content.get(),
        ) {
            Ok(futs) => {
                futs.into_iter().for_each(|f| cx.spawn(f));
            }
            Err(e) => {
                crate::log::error(format!("{e}"), Some(TryNodeAction::SendMessage));
            }
        }
        menu_selection.write().at_new_msg();
    };

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
                            td { style: "width: 100%", "{user_alias}" }
                        }
                        tr {
                            th { "To"}
                            td { style: "width: 100%", contenteditable: true, oninput: move |ev| { to.set(ev.value.clone()); } }
                        }
                        tr {
                            th { "Title"}
                            td { style: "width: 100%", contenteditable: true, oninput: move |ev| { title.set(ev.value.clone()); }  }
                        }
                    }
                }
            }
            div {
                class: "box",
                div {
                    contenteditable: true,
                    oninput: move |ev| { content.set(ev.value.clone()); },
                    br {}
                }
            }
            div {
                button {
                    class: "button is-info is-outlined",
                    onclick: send_msg,
                    "Send"
                }
            }
        }
    })
}

pub(crate) async fn error_handling(
    mut error_channel: NodeResponses,
    res: Result<(), DynError>,
    action: TryNodeAction,
) {
    if let Err(error) = res {
        crate::log::error(format!("{error}"), Some(action.clone()));
        error_channel
            .send(Err((error, action)))
            .await
            .expect("error channel closed");
    } else {
        error_channel
            .send(Ok(()))
            .await
            .expect("error channel closed");
    }
}
