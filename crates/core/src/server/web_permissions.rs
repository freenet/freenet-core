use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Form,
    extract::{Path as AxumPath, RawQuery, State},
    response::{Html, IntoResponse, Redirect},
};
use freenet_stdlib::prelude::ContractInstanceId;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::{ApiVersion, client_api::Config, errors::WebSocketApiError};

/// Sandbox tokens granted to every hosted webapp without a manifest prompt.
pub(crate) const TIER1_SANDBOX: &[&str] = &[
    "allow-scripts",
    "allow-forms",
    "allow-popups",
    "allow-downloads",
    "allow-modals",
];
/// Permissions-policy tokens granted to every hosted webapp without a manifest prompt.
pub(crate) const TIER1_ALLOW: &[&str] = &[
    "clipboard-read",
    "clipboard-write",
    "notifications",
    "persistent-storage",
    "fullscreen",
    "autoplay",
    "picture-in-picture",
];

const TIER2_SANDBOX: &[&str] = &["allow-same-origin"];
const TIER2_ALLOW: &[&str] = &[
    "microphone",
    "camera",
    "geolocation",
    "midi",
    "usb",
    "serial",
    "hid",
    "bluetooth",
    "payment",
    "display-capture",
    "xr-spatial-tracking",
];

/// File-backed store for per-contract webapp permission grants.
#[derive(Clone, Debug)]
pub(crate) struct WebAppPermissionStore {
    root: Arc<PathBuf>,
    write_lock: Arc<Mutex<()>>,
}

impl WebAppPermissionStore {
    pub(crate) fn new(data_dir: PathBuf) -> Self {
        Self {
            root: Arc::new(data_dir.join("webapp-permissions")),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    fn record_path(&self, contract_id: &ContractInstanceId) -> PathBuf {
        self.root.join(format!("{}.toml", contract_id.encode()))
    }

    async fn load(&self, contract_id: &ContractInstanceId) -> PermissionRecord {
        let path = self.record_path(contract_id);
        match tokio::fs::read_to_string(path).await {
            Ok(contents) => toml::from_str(&contents).unwrap_or_default(),
            Err(_) => PermissionRecord::default(),
        }
    }

    async fn save(
        &self,
        contract_id: &ContractInstanceId,
        record: &PermissionRecord,
    ) -> Result<(), WebSocketApiError> {
        tokio::fs::create_dir_all(self.root.as_ref())
            .await
            .map_err(|err| WebSocketApiError::NodeError {
                error_cause: format!("Failed to create webapp permissions directory: {err}"),
            })?;
        let bytes = toml::to_string(record).map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("Failed to serialize webapp permission record: {err}"),
        })?;
        tokio::fs::write(self.record_path(contract_id), bytes)
            .await
            .map_err(|err| WebSocketApiError::NodeError {
                error_cause: format!("Failed to write webapp permission record: {err}"),
            })
    }

    async fn update(
        &self,
        contract_id: &ContractInstanceId,
        update_record: impl FnOnce(PermissionRecord) -> PermissionRecord,
    ) -> Result<(), WebSocketApiError> {
        let _guard = self.write_lock.lock().await;
        let record = update_record(self.load(contract_id).await);
        self.save(contract_id, &record).await
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PermissionRecord {
    manifest_version: Option<String>,
    granted_at: Option<u64>,
    #[serde(default)]
    granted: CapabilityLists,
    #[serde(default)]
    seen: CapabilityLists,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CapabilityLists {
    #[serde(default)]
    sandbox: Vec<String>,
    #[serde(default)]
    allow: Vec<String>,
}

impl CapabilityLists {
    fn to_set(&self) -> CapabilitySet {
        CapabilitySet {
            sandbox: self.sandbox.iter().cloned().collect(),
            allow: self.allow.iter().cloned().collect(),
        }
    }

    fn from_set(set: &CapabilitySet) -> Self {
        Self {
            sandbox: set.sandbox.iter().cloned().collect(),
            allow: set.allow.iter().cloned().collect(),
        }
    }
}

/// Set of iframe sandbox and permissions-policy capabilities.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CapabilitySet {
    sandbox: BTreeSet<String>,
    allow: BTreeSet<String>,
}

impl CapabilitySet {
    fn is_empty(&self) -> bool {
        self.sandbox.is_empty() && self.allow.is_empty()
    }

    fn union(&self, other: &Self) -> Self {
        Self {
            sandbox: self.sandbox.union(&other.sandbox).cloned().collect(),
            allow: self.allow.union(&other.allow).cloned().collect(),
        }
    }

    fn difference(&self, other: &Self) -> Self {
        Self {
            sandbox: self.sandbox.difference(&other.sandbox).cloned().collect(),
            allow: self.allow.difference(&other.allow).cloned().collect(),
        }
    }

    fn intersection(&self, other: &Self) -> Self {
        Self {
            sandbox: self.sandbox.intersection(&other.sandbox).cloned().collect(),
            allow: self.allow.intersection(&other.allow).cloned().collect(),
        }
    }
}

/// Parsed `freenet-app.toml` webapp metadata and requested Tier 2 capabilities.
#[derive(Debug, Clone)]
pub(crate) struct WebAppManifest {
    pub(crate) name: Option<String>,
    pub(crate) version: Option<String>,
    required: CapabilitySet,
    optional: CapabilitySet,
}

/// Fully resolved iframe attributes plus any consent prompt that must be shown first.
#[derive(Debug, Clone)]
pub(crate) struct IframePermissions {
    pub(crate) sandbox_attr: String,
    pub(crate) allow_attr: String,
    pub(crate) prompt: Option<PermissionPrompt>,
}

/// Data needed to render the first-load webapp permission prompt.
#[derive(Debug, Clone)]
pub(crate) struct PermissionPrompt {
    pub(crate) name: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) contract_key: String,
    pub(crate) required: CapabilitySet,
    pub(crate) optional: CapabilitySet,
    pub(crate) denied_required: bool,
}

/// Reads and parses the optional `freenet-app.toml` from an unpacked webapp directory.
pub(super) async fn load_manifest(
    contract_dir: &Path,
) -> Result<Option<WebAppManifest>, WebSocketApiError> {
    let path = contract_dir.join("freenet-app.toml");
    let contents = match tokio::fs::read_to_string(&path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(WebSocketApiError::NodeError {
                error_cause: format!("Failed to read freenet-app.toml: {err}"),
            });
        }
    };
    let manifest: ManifestToml =
        toml::from_str(&contents).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("Invalid freenet-app.toml: {err}"),
        })?;
    Ok(Some(manifest.into_manifest()))
}

/// Computes the sandbox/allow attributes and optional consent prompt for a webapp.
pub(crate) async fn iframe_permissions(
    store: &WebAppPermissionStore,
    contract_id: &ContractInstanceId,
    contract_key: &str,
    manifest: Option<WebAppManifest>,
    force_prompt: bool,
) -> IframePermissions {
    let Some(manifest) = manifest else {
        return IframePermissions {
            sandbox_attr: sandbox_attr(&CapabilitySet::default()),
            allow_attr: allow_attr(&CapabilitySet::default()),
            prompt: None,
        };
    };

    let record = store.load(contract_id).await;
    let granted = record.granted.to_set();
    let seen = record.seen.to_set();
    let requested = manifest.required.union(&manifest.optional);
    let active_grants = requested.intersection(&granted);
    let missing_required = manifest.required.difference(&granted);
    let unseen_optional = manifest.optional.difference(&seen).difference(&granted);

    let should_prompt = force_prompt || !missing_required.is_empty() || !unseen_optional.is_empty();
    let prompt = should_prompt.then(|| PermissionPrompt {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        contract_key: contract_key.to_string(),
        required: missing_required.clone(),
        optional: unseen_optional,
        denied_required: !missing_required.is_empty() && !force_prompt,
    });

    IframePermissions {
        sandbox_attr: sandbox_attr(&active_grants),
        allow_attr: allow_attr(&active_grants),
        prompt,
    }
}

/// Handles the consent form POST for a hosted webapp.
pub(super) async fn permission_submit(
    AxumPath(key): AxumPath<String>,
    State(config): State<Config>,
    api_version: ApiVersion,
    RawQuery(query): RawQuery,
    Form(form): Form<PermissionForm>,
) -> Result<impl IntoResponse, WebSocketApiError> {
    let contract_id =
        ContractInstanceId::from_bytes(&key).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        })?;
    let contract_dir = super::path_handlers::contract_web_path(&contract_id);
    let Some(manifest) = load_manifest(&contract_dir).await? else {
        return Ok(Redirect::to(&shell_url(&key, api_version, query.as_deref())).into_response());
    };

    let requested = manifest.required.union(&manifest.optional);
    let selected_optional =
        capability_set_from_form_values(&form.optional).intersection(&manifest.optional);
    let allowed = form.action == "allow";
    config
        .permission_store
        .update(&contract_id, |mut record| {
            let old_granted = record.granted.to_set();
            let old_seen = record.seen.to_set();
            let (new_grants, new_seen) = if allowed {
                (
                    old_granted
                        .union(&manifest.required)
                        .union(&selected_optional),
                    old_seen.union(&requested),
                )
            } else {
                (old_granted, old_seen.union(&requested))
            };

            record.manifest_version = manifest.version.clone();
            record.granted_at = Some(now_unix_secs());
            record.granted = CapabilityLists::from_set(&new_grants);
            record.seen = CapabilityLists::from_set(&new_seen);
            record
        })
        .await?;

    let redirect = if allowed {
        shell_url(&key, api_version, query.as_deref())
    } else {
        add_query_param(
            &shell_url(&key, api_version, query.as_deref()),
            "permissions_denied=1",
        )
    };
    Ok(Redirect::to(&redirect).into_response())
}

/// Renders the standalone consent page shown before loading a Tier 2 webapp.
pub(crate) fn prompt_html(
    prompt: &PermissionPrompt,
    api_version: ApiVersion,
    query_string: Option<&str>,
) -> Html<String> {
    let title = prompt
        .name
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or("This Freenet app");
    let version = prompt
        .version
        .as_ref()
        .map(|version| format!("<p class=\"muted\">Version: {}</p>", html_escape(version)))
        .unwrap_or_default();
    let denied = if prompt.denied_required {
        "<p class=\"error\">This app needs permissions you have not granted.</p>"
    } else {
        ""
    };
    let required = capability_items(&prompt.required, false);
    let optional = capability_items(&prompt.optional, true);
    let action = permission_action(&prompt.contract_key, api_version, query_string);
    Html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Freenet permissions</title>
<style>
html,body{{height:100%;margin:0;font:16px system-ui,-apple-system,Segoe UI,sans-serif;background:#f7f7f5;color:#171717}}
body{{display:grid;place-items:center;padding:24px;box-sizing:border-box}}
main{{width:min(620px,100%);background:white;border:1px solid #d7d7d2;border-radius:8px;padding:24px;box-shadow:0 12px 40px rgba(0,0,0,.08)}}
h1{{font-size:22px;margin:0 0 12px}}
h2{{font-size:15px;margin:20px 0 8px}}
p{{line-height:1.45}}
.muted{{color:#666;font-size:13px;margin:4px 0}}
.error{{color:#9f1d1d;background:#fff1f1;border:1px solid #f0caca;border-radius:6px;padding:10px}}
ul{{padding-left:20px}}
li{{margin:8px 0}}
label{{display:flex;gap:10px;align-items:flex-start}}
code{{font-size:12px;background:#f2f2ef;padding:2px 4px;border-radius:4px;word-break:break-all}}
.actions{{display:flex;justify-content:flex-end;gap:12px;margin-top:24px}}
button{{border:1px solid #bbb;background:#f4f4f1;border-radius:6px;padding:9px 14px;font:inherit;cursor:pointer}}
button.primary{{background:#1d4ed8;color:white;border-color:#1d4ed8}}
</style>
</head>
<body>
<main>
<h1>{} wants additional browser permissions</h1>
{}
{}
<p class="muted">Contract: <code>{}</code></p>
<form method="post" action="{}">
{}
{}
<div class="actions">
<button type="submit" name="action" value="deny">Deny</button>
<button class="primary" type="submit" name="action" value="allow">Allow selected</button>
</div>
</form>
</main>
</body>
</html>"#,
        html_escape(title),
        version,
        denied,
        html_escape(&prompt.contract_key),
        html_escape_attr(&action),
        required,
        optional
    ))
}

/// URL-encoded consent form fields from `/__permissions`.
#[derive(Deserialize)]
pub(crate) struct PermissionForm {
    action: String,
    #[serde(default)]
    optional: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ManifestToml {
    #[serde(default)]
    webapp: ManifestWebAppToml,
}

#[derive(Debug, Default, Deserialize)]
struct ManifestWebAppToml {
    name: Option<String>,
    version: Option<String>,
    #[serde(default)]
    permissions: ManifestPermissionsToml,
}

#[derive(Debug, Default, Deserialize)]
struct ManifestPermissionsToml {
    #[serde(default)]
    required: CapabilityLists,
    #[serde(default)]
    optional: CapabilityLists,
}

impl ManifestToml {
    fn into_manifest(self) -> WebAppManifest {
        WebAppManifest {
            name: self.webapp.name,
            version: self.webapp.version,
            required: filter_tier2(self.webapp.permissions.required.to_set()),
            optional: filter_tier2(self.webapp.permissions.optional.to_set()),
        }
    }
}

fn filter_tier2(input: CapabilitySet) -> CapabilitySet {
    let allowed_sandbox = TIER2_SANDBOX.iter().copied().collect::<BTreeSet<_>>();
    let allowed_allow = TIER2_ALLOW.iter().copied().collect::<BTreeSet<_>>();
    CapabilitySet {
        sandbox: input
            .sandbox
            .into_iter()
            .filter(|cap| allowed_sandbox.contains(cap.as_str()))
            .collect(),
        allow: input
            .allow
            .into_iter()
            .filter(|cap| allowed_allow.contains(cap.as_str()))
            .collect(),
    }
}

fn sandbox_attr(granted: &CapabilitySet) -> String {
    TIER1_SANDBOX
        .iter()
        .copied()
        .map(str::to_string)
        .chain(granted.sandbox.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ")
}

fn allow_attr(granted: &CapabilitySet) -> String {
    TIER1_ALLOW
        .iter()
        .copied()
        .map(str::to_string)
        .chain(granted.allow.iter().cloned())
        .collect::<Vec<_>>()
        .join("; ")
}

fn capability_set_from_form_values(values: &[String]) -> CapabilitySet {
    let mut set = CapabilitySet::default();
    for value in values {
        if TIER2_SANDBOX.contains(&value.as_str()) {
            set.sandbox.insert(value.clone());
        } else if TIER2_ALLOW.contains(&value.as_str()) {
            set.allow.insert(value.clone());
        }
    }
    set
}

fn capability_items(set: &CapabilitySet, checkbox: bool) -> String {
    if set.is_empty() {
        return String::new();
    }
    let mut items = Vec::new();
    for cap in set.sandbox.iter().chain(set.allow.iter()) {
        let label = permission_label(cap);
        if checkbox {
            items.push(format!(
                r#"<li><label><input type="checkbox" name="optional" value="{}" checked> <span>{}</span></label></li>"#,
                html_escape_attr(cap),
                html_escape(label),
            ));
        } else {
            items.push(format!("<li>{}</li>", html_escape(label)));
        }
    }
    let heading = if checkbox {
        "<h2>Optional</h2>"
    } else {
        "<h2>Required</h2>"
    };
    format!("{heading}<ul>{}</ul>", items.join(""))
}

fn permission_label(cap: &str) -> &str {
    match cap {
        "allow-same-origin" => "Share browser storage origin with other Freenet apps",
        "microphone" => "Use the microphone",
        "camera" => "Use the camera",
        "geolocation" => "Access location",
        "midi" => "Use MIDI devices",
        "usb" => "Use USB devices",
        "serial" => "Use serial devices",
        "hid" => "Use HID devices",
        "bluetooth" => "Use Bluetooth devices",
        "payment" => "Use browser payment APIs",
        "display-capture" => "Capture the screen",
        "xr-spatial-tracking" => "Use XR spatial tracking",
        other => other,
    }
}

fn permission_action(
    contract_key: &str,
    api_version: ApiVersion,
    query_string: Option<&str>,
) -> String {
    let base = format!(
        "/{}/contract/web/{contract_key}/__permissions",
        api_version.prefix()
    );
    if let Some(query_string) = sanitized_query(query_string) {
        format!("{base}?{query_string}")
    } else {
        base
    }
}

fn shell_url(contract_key: &str, api_version: ApiVersion, query_string: Option<&str>) -> String {
    let base = format!("/{}/contract/web/{contract_key}/", api_version.prefix());
    if let Some(query_string) = sanitized_query(query_string) {
        format!("{base}?{query_string}")
    } else {
        base
    }
}

fn sanitized_query(query_string: Option<&str>) -> Option<String> {
    let query = query_string?;
    let params = query
        .split('&')
        .filter(|param| {
            !param.is_empty()
                && !param.starts_with("__sandbox")
                && !param.starts_with("authToken")
                && !param.starts_with("permissions_denied")
                && *param != "permissions=retry"
        })
        .map(str::to_string)
        .collect::<Vec<_>>();
    (!params.is_empty()).then(|| params.join("&"))
}

fn add_query_param(url: &str, param: &str) -> String {
    if url.contains('?') {
        format!("{url}&{param}")
    } else {
        format!("{url}?{param}")
    }
}

fn now_unix_secs() -> u64 {
    // Consent records are user-facing audit state, so they need wall-clock
    // timestamps rather than the node's simulation/test time source.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn html_escape(s: &str) -> String {
    s.chars()
        .flat_map(|ch| match ch {
            '&' => "&amp;".chars().collect::<Vec<_>>(),
            '<' => "&lt;".chars().collect::<Vec<_>>(),
            '>' => "&gt;".chars().collect::<Vec<_>>(),
            '"' => "&quot;".chars().collect::<Vec<_>>(),
            '\'' => "&#x27;".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}

fn html_escape_attr(s: &str) -> String {
    html_escape(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, http::header::LOCATION, response::IntoResponse};

    fn valid_key() -> &'static str {
        "EqJ5YpEEV3XLqEvKWLQHFhGAac2qXzSUoE6k2zbdnXBr"
    }

    fn valid_key_alt() -> &'static str {
        "HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD"
    }

    fn valid_key_concurrent() -> &'static str {
        "1thX6LZfHDZZKUs92febYZhYRcXddmzfzF2NvTkPNE"
    }

    async fn write_manifest(contract_id: &ContractInstanceId, manifest: &str) {
        let contract_dir = super::super::path_handlers::contract_web_path(contract_id);
        tokio::fs::remove_dir_all(&contract_dir).await.ok();
        tokio::fs::create_dir_all(&contract_dir).await.unwrap();
        tokio::fs::write(contract_dir.join("freenet-app.toml"), manifest)
            .await
            .unwrap();
    }

    async fn remove_manifest_dir(contract_id: &ContractInstanceId) {
        tokio::fs::remove_dir_all(super::super::path_handlers::contract_web_path(contract_id))
            .await
            .ok();
    }

    fn test_config(store: WebAppPermissionStore) -> Config {
        Config {
            localhost: true,
            permission_store: store,
        }
    }

    #[test]
    fn manifest_filters_to_tier2_only() {
        let manifest: ManifestToml = toml::from_str(
            r#"
[webapp]
name = "Mail"
version = "0.2.0"

[webapp.permissions.required]
sandbox = ["allow-same-origin", "allow-top-navigation"]
allow = ["microphone", "notifications"]
"#,
        )
        .unwrap();

        let parsed = manifest.into_manifest();
        assert!(parsed.required.sandbox.contains("allow-same-origin"));
        assert!(!parsed.required.sandbox.contains("allow-top-navigation"));
        assert!(parsed.required.allow.contains("microphone"));
        assert!(!parsed.required.allow.contains("notifications"));
    }

    #[test]
    fn tier1_defaults_include_expected_browser_features() {
        assert!(sandbox_attr(&CapabilitySet::default()).contains("allow-downloads"));
        let allow = allow_attr(&CapabilitySet::default());
        assert!(allow.contains("clipboard-read"));
        assert!(allow.contains("notifications"));
        assert!(allow.contains("persistent-storage"));
        assert!(allow.contains("fullscreen"));
    }

    #[tokio::test]
    async fn no_manifest_uses_tier1_defaults_without_prompt() {
        let temp = tempfile::tempdir().unwrap();
        let store = WebAppPermissionStore::new(temp.path().to_path_buf());
        let key = valid_key();
        let contract_id = ContractInstanceId::from_bytes(key).unwrap();

        let permissions = iframe_permissions(&store, &contract_id, key, None, false).await;

        assert!(permissions.prompt.is_none());
        assert_eq!(permissions.sandbox_attr, TIER1_SANDBOX.join(" "));
        assert_eq!(permissions.allow_attr, TIER1_ALLOW.join("; "));
    }

    #[tokio::test]
    async fn missing_required_permission_renders_prompt_not_iframe_grant() {
        let temp = tempfile::tempdir().unwrap();
        let store = WebAppPermissionStore::new(temp.path().to_path_buf());
        let key = valid_key();
        let contract_id = ContractInstanceId::from_bytes(key).unwrap();
        let manifest = WebAppManifest {
            name: Some("Mail".to_string()),
            version: Some("0.2.0".to_string()),
            required: CapabilitySet {
                sandbox: ["allow-same-origin".to_string()].into_iter().collect(),
                allow: BTreeSet::new(),
            },
            optional: CapabilitySet::default(),
        };

        let permissions =
            iframe_permissions(&store, &contract_id, key, Some(manifest), false).await;

        assert!(
            permissions.prompt.is_some(),
            "missing required tier-2 capability should render a consent prompt"
        );
        assert!(
            !permissions.sandbox_attr.contains("allow-same-origin"),
            "ungranted allow-same-origin must not reach the iframe"
        );
    }

    #[tokio::test]
    async fn force_prompt_retries_without_denied_required_banner() {
        let temp = tempfile::tempdir().unwrap();
        let store = WebAppPermissionStore::new(temp.path().to_path_buf());
        let key = valid_key();
        let contract_id = ContractInstanceId::from_bytes(key).unwrap();
        let manifest = WebAppManifest {
            name: Some("Mail".to_string()),
            version: Some("0.2.0".to_string()),
            required: CapabilitySet {
                sandbox: ["allow-same-origin".to_string()].into_iter().collect(),
                allow: BTreeSet::new(),
            },
            optional: CapabilitySet::default(),
        };

        let permissions = iframe_permissions(&store, &contract_id, key, Some(manifest), true).await;

        let prompt = permissions
            .prompt
            .expect("force_prompt should render the consent prompt");
        assert!(
            !prompt.denied_required,
            "retry prompts should not show the prior-denial error banner"
        );
        assert!(
            !permissions.sandbox_attr.contains("allow-same-origin"),
            "retry prompt must not grant missing required caps before consent"
        );
    }

    #[tokio::test]
    async fn stored_grant_extends_iframe_attributes() {
        let temp = tempfile::tempdir().unwrap();
        let store = WebAppPermissionStore::new(temp.path().to_path_buf());
        let key = valid_key_alt();
        let contract_id = ContractInstanceId::from_bytes(key).unwrap();
        let grant = CapabilitySet {
            sandbox: ["allow-same-origin".to_string()].into_iter().collect(),
            allow: ["microphone".to_string()].into_iter().collect(),
        };
        let record = PermissionRecord {
            manifest_version: Some("0.2.0".to_string()),
            granted_at: Some(1),
            granted: CapabilityLists::from_set(&grant),
            seen: CapabilityLists::from_set(&grant),
        };
        store.save(&contract_id, &record).await.unwrap();
        let manifest = WebAppManifest {
            name: Some("Mail".to_string()),
            version: Some("0.2.0".to_string()),
            required: CapabilitySet {
                sandbox: ["allow-same-origin".to_string()].into_iter().collect(),
                allow: BTreeSet::new(),
            },
            optional: CapabilitySet {
                sandbox: BTreeSet::new(),
                allow: ["microphone".to_string()].into_iter().collect(),
            },
        };

        let permissions =
            iframe_permissions(&store, &contract_id, key, Some(manifest), false).await;

        assert!(permissions.prompt.is_none());
        assert!(permissions.sandbox_attr.contains("allow-same-origin"));
        assert!(permissions.allow_attr.contains("microphone"));
    }

    #[tokio::test]
    async fn permission_submit_allows_selected_tier2_and_drops_forged_values() {
        let temp = tempfile::tempdir().unwrap();
        let store = WebAppPermissionStore::new(temp.path().to_path_buf());
        let key = valid_key_alt();
        let contract_id = ContractInstanceId::from_bytes(key).unwrap();
        write_manifest(
            &contract_id,
            r#"
[webapp]
name = "Mail"
version = "0.2.0"

[webapp.permissions.required]
sandbox = ["allow-same-origin"]
allow = []

[webapp.permissions.optional]
sandbox = []
allow = ["microphone", "camera"]
"#,
        )
        .await;

        let response = permission_submit(
            axum::extract::Path(key.to_string()),
            State(test_config(store.clone())),
            ApiVersion::V1,
            axum::extract::RawQuery(Some("invite=abc&authToken=evil".to_string())),
            Form(PermissionForm {
                action: "allow".to_string(),
                optional: vec![
                    "microphone".to_string(),
                    "allow-top-navigation".to_string(),
                    "usb".to_string(),
                ],
            }),
        )
        .await
        .unwrap()
        .into_response();

        let record = store.load(&contract_id).await;
        let granted = record.granted.to_set();
        assert!(granted.sandbox.contains("allow-same-origin"));
        assert!(granted.allow.contains("microphone"));
        assert!(!granted.allow.contains("usb"));
        assert!(!granted.allow.contains("camera"));
        assert!(!granted.sandbox.contains("allow-top-navigation"));
        let location = response.headers().get(LOCATION).unwrap().to_str().unwrap();
        assert_eq!(location, format!("/v1/contract/web/{key}/?invite=abc"));

        remove_manifest_dir(&contract_id).await;
    }

    #[tokio::test]
    async fn concurrent_permission_submits_preserve_both_grants() {
        let temp = tempfile::tempdir().unwrap();
        let store = WebAppPermissionStore::new(temp.path().to_path_buf());
        let key = valid_key_concurrent();
        let contract_id = ContractInstanceId::from_bytes(key).unwrap();
        write_manifest(
            &contract_id,
            r#"
[webapp]
name = "Mail"
version = "0.2.0"

[webapp.permissions.optional]
sandbox = []
allow = ["microphone", "camera"]
"#,
        )
        .await;

        let submit_microphone = permission_submit(
            axum::extract::Path(key.to_string()),
            State(test_config(store.clone())),
            ApiVersion::V1,
            axum::extract::RawQuery(None),
            Form(PermissionForm {
                action: "allow".to_string(),
                optional: vec!["microphone".to_string()],
            }),
        );
        let submit_camera = permission_submit(
            axum::extract::Path(key.to_string()),
            State(test_config(store.clone())),
            ApiVersion::V1,
            axum::extract::RawQuery(None),
            Form(PermissionForm {
                action: "allow".to_string(),
                optional: vec!["camera".to_string()],
            }),
        );

        let (microphone_response, camera_response) = tokio::join!(submit_microphone, submit_camera);
        microphone_response.unwrap();
        camera_response.unwrap();

        let record = store.load(&contract_id).await;
        let granted = record.granted.to_set();
        assert!(granted.allow.contains("microphone"));
        assert!(granted.allow.contains("camera"));

        remove_manifest_dir(&contract_id).await;
    }

    #[tokio::test]
    async fn permission_submit_denies_without_granting_required_caps() {
        let temp = tempfile::tempdir().unwrap();
        let store = WebAppPermissionStore::new(temp.path().to_path_buf());
        let key = valid_key();
        let contract_id = ContractInstanceId::from_bytes(key).unwrap();
        write_manifest(
            &contract_id,
            r#"
[webapp]
name = "Mail"
version = "0.2.0"

[webapp.permissions.required]
sandbox = ["allow-same-origin"]
allow = []

[webapp.permissions.optional]
sandbox = []
allow = ["camera"]
"#,
        )
        .await;

        let response = permission_submit(
            axum::extract::Path(key.to_string()),
            State(test_config(store.clone())),
            ApiVersion::V2,
            axum::extract::RawQuery(Some("permissions=retry&room=42".to_string())),
            Form(PermissionForm {
                action: "deny".to_string(),
                optional: vec!["camera".to_string()],
            }),
        )
        .await
        .unwrap()
        .into_response();

        let record = store.load(&contract_id).await;
        let granted = record.granted.to_set();
        let seen = record.seen.to_set();
        assert!(granted.is_empty());
        assert!(seen.sandbox.contains("allow-same-origin"));
        assert!(seen.allow.contains("camera"));
        let location = response.headers().get(LOCATION).unwrap().to_str().unwrap();
        assert_eq!(
            location,
            format!("/v2/contract/web/{key}/?room=42&permissions_denied=1")
        );

        remove_manifest_dir(&contract_id).await;
    }
}
