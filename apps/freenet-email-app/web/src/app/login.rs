use std::{cell::RefCell, rc::Rc};

use dioxus::prelude::*;
use identity_management::IdentityManagement;
use locutus_stdlib::prelude::ContractKey;
use once_cell::unsync::Lazy;
use rand::rngs::OsRng;
use rsa::pkcs1::EncodeRsaPublicKey;
use rsa::RsaPrivateKey;

use crate::api::AliasInfo;
use crate::app::{ContractType, User, UserId};
use crate::DynError;

use super::{Identity, InboxView, NodeAction};

const DEFAULT_ID_ICON: &str = "data:image/x-icon;base64,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";
const RSA_KEY_SIZE: usize = 4096;

struct ImportId(bool);

struct CreateAlias(bool);

fn login_header(cx: Scope) -> Element {
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

thread_local! {
    static ALIASES: Lazy<Rc<RefCell<Vec<Alias>>>> = Lazy::new(|| {
        Rc::new(RefCell::new(Vec::default()))
    });
}

#[derive(PartialEq, Eq, Clone)]
pub(crate) struct Alias {
    pub alias: Rc<str>,
    id: UserId,
    info: Rc<AliasInfo>,
    pub key: RsaPrivateKey,
}

impl Alias {
    #[must_use]
    pub(crate) fn set_aliases(
        mut new_aliases: IdentityManagement,
        user: &UseSharedState<crate::app::User>,
    ) -> Vec<Identity> {
        ALIASES.with(|aliases| {
            let aliases = &mut *aliases.borrow_mut();
            let mut to_add = Vec::new();
            for alias in &*aliases {
                // just modify and avoid creating a new id
                let key: RsaPrivateKey = serde_json::from_slice(&alias.info.key).unwrap();
                if let Some(info) = new_aliases.remove(&alias.alias) {
                    to_add.push(Alias {
                        alias: alias.alias.clone(),
                        id: alias.id,
                        info: Rc::new(AliasInfo {
                            alias: alias.alias.clone(),
                            description: info.extra.unwrap_or_default(),
                            key: info.key,
                        }),
                        key,
                    });
                }
            }
            let mut identities = Vec::new();
            new_aliases.into_info().for_each(|(alias, info)| {
                let key: RsaPrivateKey = serde_json::from_slice(&info.key).unwrap();
                let alias: Rc<str> = alias.into();
                let id = UserId::new();
                let identity = Identity {
                    id,
                    key: key.clone(),
                    alias: alias.clone(),
                };
                user.write().identities.push(identity.clone());
                to_add.push(Alias {
                    alias: alias.clone(),
                    id,
                    info: Rc::new(AliasInfo {
                        alias,
                        description: info.extra.unwrap_or_default(),
                        key: info.key,
                    }),
                    key,
                });
                identities.push(identity);
            });
            *aliases = to_add;
            identities
        })
    }

    pub(crate) fn set_alias(
        alias_info: AliasInfo,
        inbox_key: ContractKey,
        user: &UseSharedState<crate::app::User>,
    ) -> Identity {
        let private_key: RsaPrivateKey = serde_json::from_slice(&alias_info.key).unwrap();
        let identity = Identity {
            id: UserId::new(),
            key: private_key.clone(),
            alias: alias_info.alias.clone(),
        };
        crate::inbox::InboxModel::set_contract_identity(inbox_key, identity.clone());
        user.write().identities.push(identity.clone());
        ALIASES.with(|aliases| {
            let aliases = &mut *aliases.borrow_mut();
            aliases.push(Alias {
                alias: alias_info.alias.clone(),
                id: identity.id,
                info: Rc::new(alias_info),
                key: private_key,
            });
        });
        identity
    }

    pub(crate) fn get_aliases() -> Rc<RefCell<Vec<Alias>>> {
        ALIASES.with(|aliases| (**aliases).clone())
    }

    pub(crate) fn get_alias(alias: impl AsRef<str>) -> Option<Alias> {
        let alias = alias.as_ref();
        ALIASES.with(|aliases: &Lazy<Rc<RefCell<Vec<Alias>>>>| {
            let aliases = &*aliases.borrow();
            aliases.iter().find(|a| &*a.alias == alias).cloned()
        })
    }
}

#[derive(Debug, Clone)]
pub struct LoginController {
    pub updated: bool,
}

impl LoginController {
    pub fn new() -> Self {
        Self { updated: false }
    }
}

pub(super) fn identifiers_list(cx: Scope) -> Element {
    use_shared_state_provider::<CreateAlias>(cx, || CreateAlias(false));
    let create_alias_form = use_shared_state::<CreateAlias>(cx).unwrap();
    let actions = use_coroutine_handle::<NodeAction>(cx).unwrap();

    cx.render(rsx! {
        login_header {}
        div {
            class: "columns",
            div { class: "column is-3" }
            div {
                class: "column is-6",
                div {
                    class: "card has-background-light is-small mt-2",
                    identities(cx)
                    if create_alias_form.read().0 {
                        create_alias(cx, actions)
                    }
                }
            }
        }
    })
}

pub(super) fn identities(cx: Scope) -> Element {
    let aliases = Alias::get_aliases();
    let aliases_list = aliases.borrow();
    let create_alias_form = use_shared_state::<CreateAlias>(cx).unwrap();
    let login_controller = use_shared_state::<LoginController>(cx).unwrap();

    if login_controller.read().updated {
        login_controller.write_silent().updated = false;
    }

    #[inline_props]
    fn identity_entry(cx: Scope, alias: Rc<str>, info: Rc<AliasInfo>, id: UserId) -> Element {
        let user = use_shared_state::<User>(cx).unwrap();
        let inbox = use_shared_state::<InboxView>(cx).unwrap();
        let description = &info.description;
        cx.render(rsx! {
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
                                onclick: move |_| {
                                    user.write().set_logged_id(*id);
                                    inbox.write().set_active_id(*id);
                                },
                                "{alias}"
                            }
                        },
                        p { class: "subtitle is-6", "{description}" }
                    }
                }
            }
        })
    }

    let identities = aliases_list.iter().map(|alias| {
        rsx!(identity_entry {
            alias: alias.alias.clone(),
            info: alias.info.clone(),
            id: alias.id
        })
    });

    cx.render(rsx! {
        identities
        // div {
        //     class: "card-content",
        //     div {
        //         class: "media",
        //         div {
        //             class: "media-left",
        //             figure { class: "image is-48x48", img { src: DEFAULT_ID_ICON } }
        //         }
        //         div {
        //             class: "media-content",
        //             p {
        //                 class: "title is-4",
        //                 a {
        //                     style: "color: inherit",
        //                     onclick: move |_| {
        //                         let id = UserId::new(0);
        //                         user.write().set_logged_id(id);
        //                         inbox.set_active_id(id);
        //                     },
        //                     "Ian Clarke"
        //                 }
        //             },
        //             p { class: "subtitle is-6", "ian.clarke@freenet.org" }
        //         }
        //     }
        // },
        // div {
        //     class: "card-content",
        //     div {
        //         class: "media",
        //         div {
        //             class: "media-left",
        //             figure { class: "image is-48x48", img { src: DEFAULT_ID_ICON } }
        //         }
        //         div {
        //             class: "media-content",
        //             p {
        //                 class: "title is-4",
        //                 a {
        //                     style: "color: inherit",
        //                     onclick: move |_| {
        //                         let id = UserId::new(1);
        //                         user.write().set_logged_id(id);
        //                         inbox.set_active_id(id);
        //                     },
        //                     "Ian's Other Account"
        //                 }
        //             },
        //             p { class: "subtitle is-6", "other.stuff@freenet.org" }
        //         }
        //     }
        // },
        div {
            class: "card-content columns",
            div { class: "column is-4" }
            a {
                class: "column is-4 is-link",
                onclick: move |_| {
                    create_alias_form.write().0 = true;
                },
                "Create new alias"
            }
        }
    })
}

pub(super) fn create_alias<'x>(cx: Scope<'x>, actions: &'x Coroutine<NodeAction>) -> Element<'x> {
    let create_alias_form: &UseSharedState<CreateAlias> =
        use_shared_state::<CreateAlias>(cx).unwrap();
    let login_controller = use_shared_state::<LoginController>(cx).unwrap();

    if login_controller.read().updated {
        login_controller.write_silent().updated = false;
    }

    let generate = use_state(cx, || true);
    let address = use_state(cx, String::new);
    let description = use_state(cx, String::new);
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
                label { "Alias" }
                div {
                    class: "control has-icons-left",
                    input {
                        class: "input",
                        placeholder: "Address",
                        value: "{address}",
                        oninput: move |evt| address.modify(|_| evt.value.clone())
                    }
                    span { class: "icon is-small is-left", i { class: "fas fa-envelope" } }
                }
            }
            div {
                class: "field",
                label { "Description" }
                div {
                    class: "control has-icons-left",
                    input {
                        class: "input",
                        placeholder: "",
                        value: "{description}",
                        oninput: move |evt| description.modify(|_| evt.value.clone())
                    }
                    span { class: "icon is-small is-left", i { class: "fas fa-envelope" } }
                }
            }
            div {
                class: "columns mb-2 mt-2",
                div {
                    class: "column is-two-fifths",
                    div {
                        class: "file is-small has-name",
                        label {
                            class: "file-label",
                            input { class: "file-input", r#type: "file", name: "keypair-file" }
                            span {
                                class: "file-cta",
                                span { class: "file-icon", i { class: "fas fa-upload" } }
                                span { class: "file-label", "Import key file" }
                            }
                            span { class: "file-name has-background-white", "{key_path}" }
                        }
                    }
                }
                div {
                    class:"column is-one-fifth" ,
                    p { class: "has-text-centered", "or" }
                }
                div {
                    class: "column is-two-fifths",

                    label {
                        class: "checkbox",
                        input {
                            r#type: "checkbox",
                            checked: true,
                            onclick: move |_| {
                                generate.modify(|current| !current);
                            }
                        },
                        "  generate"
                    }
                }
            }
            a {
                class: "button",
                onclick: move |_|  {
                    create_alias_form.write().0 = false;
                    let alias: Rc<str> = address.get().to_owned().into();
                    // Generate or import keypair
                    let key = match get_key(generate) {
                        Ok(k) => k,
                        Err(e) => {
                            crate::log::debug!("Failed to generate or import key: {:?}", e);
                            vec![]
                        }
                    };
                    // - create inbox contract
                    actions.send(NodeAction::CreateContract {
                        alias: alias.clone(),
                        key: key.clone(),
                        contract_type: ContractType::InboxContract,
                    });
                    // - create AFT delegate && contract
                    actions.send(NodeAction::CreateDelegate {
                        alias: alias.clone(),
                        key: key.clone(),
                    });
                    actions.send(NodeAction::CreateContract {
                        alias: alias.clone(),
                        key: key.clone(),
                        contract_type: ContractType::AFTContract,
                    });

                    let description = description.get().into();
                    actions.send(NodeAction::CreateIdentity { alias, key, description });
                },
                "Create"
            }
        }
    })
}

fn get_key(generate: &UseState<bool>) -> Result<Vec<u8>, DynError> {
    if *generate.get() {
        crate::log::debug!("generating keypair");
        let private_key =
            RsaPrivateKey::new(&mut OsRng, RSA_KEY_SIZE).expect("failed to generate keypair");
        crate::log::debug!(
            "generated public key: {key}",
            key = private_key
                .to_public_key()
                .to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)
                .unwrap()
        );
        Ok(serde_json::to_vec(&private_key)?)
    } else {
        crate::log::debug!("importing keypair");
        Err("importing not implemented yet".into())
    }
}

pub(super) fn get_or_create_indentity(cx: Scope) -> Element {
    use_shared_state_provider::<ImportId>(cx, || ImportId(false));
    let import_form_state = use_shared_state::<ImportId>(cx).unwrap();
    cx.render(rsx! {
        login_header {}
        div {
            class: "columns",
            div { class: "column is-4"},
            div {
                class: "column is-4",
                if !import_form_state.read().0 {
                    create_links(cx)
                } else {
                    import_form(cx)
                }
            }
        }
    })
}

fn create_links(cx: Scope) -> Element {
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

fn import_form(cx: Scope) -> Element {
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
