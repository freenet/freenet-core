use std::fmt::Display;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::{borrow::Cow, cell::RefCell, rc::Rc};

use arc_swap::ArcSwap;
use chrono::Utc;
use dioxus::prelude::*;
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use rsa::{RsaPrivateKey, RsaPublicKey};
use wasm_bindgen::JsValue;

pub(crate) use login::{Identity, LoginController};

use crate::api::{node_response_error_handling, TryNodeAction};
use crate::{
    api::WebApiRequestClient,
    inbox::{DecryptedMessage, InboxModel, MessageModel},
    DynError,
};

mod login;

#[derive(Clone, Debug)]
pub(crate) enum NodeAction {
    LoadMessages(Box<Identity>),
    CreateIdentity {
        alias: Rc<str>,
        key: RsaPrivateKey,
        description: String,
    },
    CreateContract {
        alias: Rc<str>,
        contract_type: ContractType,
        key: RsaPrivateKey,
    },
    CreateDelegate {
        alias: Rc<str>,
        key: RsaPrivateKey,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum ContractType {
    InboxContract,
    AFTContract,
}

impl Display for ContractType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContractType::InboxContract => write!(f, "InboxContract"),
            ContractType::AFTContract => write!(f, "AFTContract"),
        }
    }
}

pub(crate) fn app(cx: Scope) -> Element {
    use_shared_state_provider(cx, login::LoginController::new);
    let login_controller = use_shared_state::<login::LoginController>(cx).unwrap();
    // Initialize and fetch shared state for User, InboxController, and InboxView
    use_shared_state_provider(cx, User::new);
    let user = use_shared_state::<User>(cx).unwrap();
    use_shared_state_provider(cx, InboxController::new);
    let inbox_controller = use_shared_state::<InboxController>(cx).unwrap();
    use_shared_state_provider(cx, InboxView::new);
    let inbox = use_shared_state::<InboxView>(cx).unwrap();
    use_context_provider(cx, InboxesData::new);
    let inbox_data = use_context::<InboxesData>(cx).unwrap();

    #[cfg(feature = "use-node")]
    {
        let _sync: &Coroutine<NodeAction> = use_coroutine::<NodeAction, _, _>(cx, move |rx| {
            to_owned![inbox_controller];
            to_owned![login_controller];
            to_owned![user];
            let fut = crate::api::node_comms(
                rx,
                inbox_controller,
                login_controller,
                user,
                inbox_data.clone(),
            )
            .map(|_| Ok(JsValue::NULL));
            let _ = wasm_bindgen_futures::future_to_promise(fut);
            async {}.boxed_local()
        });
    }
    #[cfg(not(feature = "use-node"))]
    {
        let _sync = use_coroutine::<NodeAction, _, _>(cx, move |rx| async {});
    }
    let actions = use_coroutine_handle::<NodeAction>(cx).unwrap();

    // Render login page if user not identified, otherwise render the inbox or identifiers list based on user's logged in state
    if !user.read().identified {
        cx.render(rsx! {
            login::get_or_create_indentity {}
        })
    } else if let Some(id) = user.read().logged_id() {
        #[cfg(feature = "use-node")]
        {
            inbox
                .read()
                .load_messages(id, actions)
                .expect("load messages");
        }
        #[cfg(all(feature = "ui-testing", not(feature = "use-node")))]
        {
            inbox_controller.load_messages(id).unwrap();
        }
        cx.render(rsx! {
           user_inbox {}
        })
    } else {
        cx.render(rsx! {
           login::identifiers_list {}
        })
    }
}

#[derive(Clone)]
pub(crate) struct InboxesData(Arc<ArcSwap<Vec<Rc<RefCell<InboxModel>>>>>);

impl InboxesData {
    pub fn new() -> Self {
        Self(Arc::new(ArcSwap::from_pointee(vec![])))
    }
}

impl std::ops::Deref for InboxesData {
    type Target = Arc<ArcSwap<Vec<Rc<RefCell<InboxModel>>>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct InboxView {
    active_id: Rc<RefCell<UserId>>,
    /// loaded messages for the currently selected `active_id`
    messages: Rc<RefCell<Vec<Message>>>,
}

#[derive(Debug, Clone)]
pub struct InboxController {
    pub updated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct UserId(usize);

impl UserId {
    pub fn new() -> Self {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        Self(NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

impl std::ops::Deref for UserId {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl InboxController {
    pub fn new() -> Self {
        Self { updated: false }
    }
}

impl InboxView {
    fn new() -> Self {
        Self {
            messages: Rc::new(RefCell::new(vec![])),
            active_id: Rc::new(RefCell::new(UserId(0))),
        }
    }

    fn set_active_id(&mut self, user: UserId) {
        let mut id = self.active_id.borrow_mut();
        *id = user;
    }

    fn send_message(
        &mut self,
        client: WebApiRequestClient,
        from: &str,
        to: RsaPublicKey,
        title: &str,
        content: &str,
    ) -> Result<Vec<LocalBoxFuture<'static, ()>>, DynError> {
        tracing::debug!("sending message from {from}");
        let content = DecryptedMessage {
            title: title.to_owned(),
            content: content.to_owned(),
            from: from.to_owned(),
            to: vec![to],
            cc: vec![],
            time: Utc::now(),
        };
        let mut futs = Vec::with_capacity(content.to.len());
        #[cfg(feature = "use-node")]
        {
            crate::log::debug!("sending message from {from}");
            for recipient_encoded_key in content.to.iter() {
                let content = content.clone();
                let mut client = client.clone();
                let Some(id) = crate::inbox::InboxModel::id_for_alias(from) else {
                    crate::log::error(
                        format!("alias `{from}` not stored"),
                        Some(TryNodeAction::SendMessage),
                    );
                    continue;
                };
                let to = recipient_encoded_key.clone();
                let f = async move {
                    let res = content.start_sending(&mut client, to, &id).await;
                    node_response_error_handling(client.into(), res, TryNodeAction::SendMessage)
                        .await;
                };
                futs.push(f.boxed_local());
            }
        }
        let _ = client;
        Ok(futs)
    }

    fn remove_messages(
        &mut self,
        client: WebApiRequestClient,
        ids: &[u64],
        inbox_data: InboxesData,
    ) -> Result<LocalBoxFuture<'static, ()>, DynError> {
        tracing::debug!("removing messages: {ids:?}");
        // FIXME: indexing by id fails cause all not aliases inboxes has been loaded initially
        let inbox_data = inbox_data.load_full();
        let mut inbox = inbox_data[**self.active_id.borrow()].borrow_mut();
        inbox.remove_messages(client, ids)
    }

    // Remove the messages from the inbox contract, and move them to local storage
    fn mark_as_read(
        &mut self,
        client: WebApiRequestClient,
        ids: &[u64],
        inbox_data: InboxesData,
    ) -> Result<LocalBoxFuture<'static, ()>, DynError> {
        {
            let messages = &mut *self.messages.borrow_mut();
            let mut removed_messages = Vec::with_capacity(ids.len());
            for e in messages {
                if ids.contains(&e.id) {
                    e.read = true;
                    let m = e.clone();
                    removed_messages.push(m);
                }
            }
        }
        // todo: persist in a delegate `removed_messages`
        self.remove_messages(client, ids, inbox_data)
    }

    #[cfg(all(feature = "ui-testing", not(feature = "use-node")))]
    fn load_messages(&self, id: &Identity) -> Result<(), DynError> {
        let emails = {
            if id.id == UserId(0) {
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
    fn load_messages(
        &self,
        id: &Identity,
        actions: &Coroutine<NodeAction>,
    ) -> Result<(), DynError> {
        actions.send(NodeAction::LoadMessages(Box::new(id.clone())));
        Ok(())
    }
}

pub(crate) struct User {
    logged: bool,
    identified: bool,
    active_id: Option<UserId>,
    pub identities: Vec<Identity>,
}

impl User {
    #[cfg(all(feature = "ui-testing", not(feature = "use-node")))]
    fn new() -> Self {
        use rand_chacha::rand_core::OsRng;
        let key0 = RsaPrivateKey::new(&mut OsRng, 4096).unwrap();
        let key1 = RsaPrivateKey::new(&mut OsRng, 4096).unwrap();
        let identified = true;
        User {
            logged: false,
            identified,
            active_id: None,
            identities: vec![
                Identity {
                    alias: "address1".into(),
                    id: UserId(0),
                    key: key0,
                },
                Identity {
                    alias: "address2".into(),
                    id: UserId(1),
                    key: key1,
                },
            ],
        }
    }

    #[cfg(feature = "use-node")]
    fn new() -> Self {
        User {
            logged: false,
            identified: true,
            active_id: None,
            identities: vec![],
        }
    }

    fn logged_id(&self) -> Option<&Identity> {
        self.active_id.and_then(|id| self.identities.get(id.0))
    }

    fn set_logged_id(&mut self, id: UserId) {
        assert!(id.0 < self.identities.len());
        self.active_id = Some(id);
    }
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
        Some(self.cmp(other))
    }
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
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

        pub fn is_inbox_list(&self) -> bool {
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

fn user_inbox(cx: Scope) -> Element {
    use_shared_state_provider(cx, menu::MenuSelection::default);
    cx.render(rsx!(
        div {
            class: "columns",
            nav {
                class: "column is-one-fifth menu",
                user_menu_component {}
            }
            div {
                class: "column",
                inbox_component {}
            }
        }
    ))
}

fn user_menu_component(cx: Scope) -> Element {
    let user = use_shared_state::<User>(cx).unwrap();
    let menu_selection = use_shared_state::<menu::MenuSelection>(cx).unwrap();

    let received_class =
        if menu_selection.read().is_inbox_list() || !menu_selection.read().is_new_msg() {
            "is-active"
        } else {
            ""
        };
    let write_msg_class = if menu_selection.read().is_new_msg() {
        "is-active"
    } else {
        ""
    };

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

fn inbox_component(cx: Scope) -> Element {
    let inbox = use_shared_state::<InboxView>(cx).unwrap();
    let controller = use_shared_state::<InboxController>(cx).unwrap();
    let inbox_data = use_context::<InboxesData>(cx).unwrap();
    let menu_selection = use_shared_state::<menu::MenuSelection>(cx).unwrap();
    let user = use_shared_state::<User>(cx).unwrap();

    #[inline_props]
    fn email_link<'a>(
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

    {
        let current_active_id: UserId = user.read().active_id.unwrap();
        // reload if there were new emails received
        let all_data = inbox_data.load_full();
        if let Some((current_model, id)) = all_data.iter().find_map(|ib| {
            crate::log::debug!("trying to get identity for {key}", key = &ib.borrow().key);
            let id = crate::inbox::InboxModel::contract_identity(&ib.borrow().key).unwrap();
            (id.id == current_active_id).then_some((ib, id))
        }) {
            let inbox = inbox.read();
            let mut emails = inbox.messages.borrow_mut();
            emails.clear();
            for msg in &current_model.borrow().messages {
                let m = Message::from(msg.clone());
                emails.push(m);
            }
            crate::log::debug!("active id: {:?}; emails number: {}", id.alias, emails.len());
        }
    }

    // Mark updated as false after after the inbox has been refreshed
    if controller.read().updated {
        controller.write_silent().updated = false;
    }

    let inbox = inbox.read();
    let emails = inbox.messages.borrow();
    let is_email: Option<u64> = menu_selection.read().email();
    if let Some(email_id) = is_email {
        let id_p = (*emails).binary_search_by_key(&email_id, |e| e.id).unwrap();
        let email = &emails[id_p];
        cx.render(rsx! {
            open_message {
                id: email.id,
                from: email.from.clone(),
                title: email.title.clone(),
                content: email.content.clone(),
                read: email.read,
            }
        })
    } else if menu_selection.read().is_new_msg() {
        cx.render(rsx! {
            new_message_window {}
        })
    } else {
        DELAYED_ACTIONS.with(|queue| {
            let mut queue = queue.borrow_mut();
            for fut in queue.drain(..) {
                cx.spawn(fut);
            }
        });
        let links = emails.iter().enumerate().map(|(id, email)| {
            rsx!(email_link {
                sender: email.from.clone(),
                title: email.title.clone(),
                read: email.read,
                id: id as u64,
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

thread_local! {
    static DELAYED_ACTIONS: RefCell<Vec<LocalBoxFuture<'static, ()>>> = RefCell::new(Vec::new());
}

fn open_message(cx: Scope<Message>) -> Element {
    let menu_selection = use_shared_state::<menu::MenuSelection>(cx).unwrap();
    let client = crate::api::WEB_API_SENDER.get().unwrap();
    let inbox = use_shared_state::<InboxView>(cx).unwrap();
    let inbox_data = use_context::<InboxesData>(cx).unwrap();
    let email = cx.props;
    let email_id = [cx.props.id];

    let result = inbox
        .write_silent()
        .mark_as_read(client.clone(), &email_id, inbox_data.clone())
        .unwrap();
    DELAYED_ACTIONS.with(|queue| {
        queue.borrow_mut().push(result);
    });

    // todo: delete this from the private delegate or send to trash category
    // let delete = move |_| {
    //     let result = inbox
    //         .write_silent()
    //         .remove_messages(client.clone(), &email_id, inbox_data.clone())
    //         .unwrap();
    //     cx.spawn(result);
    //     menu_selection.write().at_inbox_list();
    // };

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
                    // onclick: delete,
                    onclick: move |_| {},
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

fn new_message_window(cx: Scope) -> Element {
    let menu_selection = use_shared_state::<menu::MenuSelection>(cx).unwrap();
    let client = crate::api::WEB_API_SENDER.get().unwrap();
    let inbox = use_shared_state::<InboxView>(cx).unwrap();
    let user = use_shared_state::<User>(cx).unwrap();
    let user = user.read();
    let user_alias = &*user.logged_id().unwrap().alias;
    let to = use_state(cx, String::new);
    let title = use_state(cx, String::new);
    let content = use_state(cx, String::new);

    let alias = user_alias.to_string();
    let send_msg = move |_| {
        let to = to.get();
        // fixme: this will have to come from the address book in the future
        let receiver_public_key = match Identity::get_alias(to) {
            Some(v) => v.key.to_public_key(),
            None => {
                crate::log::error(
                    format!("couldn't find key for `{to}`"),
                    Some(TryNodeAction::GetAlias),
                );
                return;
            }
        };
        match inbox.write().send_message(
            client.clone(),
            &alias,
            receiver_public_key,
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
                            th { "Subject"}
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
