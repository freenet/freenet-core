use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::{ClientResponse, UserInputRequest};
use tokio::sync::oneshot;

/// Timeout for user input prompts. After this duration, the request is auto-denied.
pub(crate) const USER_INPUT_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum message length for display. Prevents abuse from untrusted delegates.
const MAX_MESSAGE_LEN: usize = 2048;
/// Maximum button label length. 64 chars fits comfortably in a button.
const MAX_LABEL_LEN: usize = 64;
/// Maximum number of response buttons. Keeps the UI usable.
const MAX_LABELS: usize = 10;

/// Abstracts user prompting for delegate `RequestUserInput` messages.
pub trait UserInputPrompter: Send + Sync {
    fn prompt(
        &self,
        request: &UserInputRequest<'static>,
    ) -> impl std::future::Future<Output = Option<(usize, ClientResponse<'static>)>> + Send;
}

/// A pending permission request awaiting user response via the web dashboard.
pub(crate) struct PendingPrompt {
    pub message: String,
    pub labels: Vec<String>,
    pub delegate_key: String,
    pub contract_id: String,
    pub response_tx: oneshot::Sender<usize>,
}

/// Shared registry of pending permission prompts, keyed by 128-bit hex nonce.
pub(crate) type PendingPrompts = Arc<DashMap<String, PendingPrompt>>;

/// Global pending prompts registry, shared between the HTTP server (consumer)
/// and the DashboardPrompter (producer).
static PENDING_PROMPTS: std::sync::OnceLock<PendingPrompts> = std::sync::OnceLock::new();

/// Get or initialize the global pending prompts registry.
pub(crate) fn pending_prompts() -> PendingPrompts {
    PENDING_PROMPTS
        .get_or_init(|| Arc::new(DashMap::new()))
        .clone()
}

/// Maximum concurrent pending prompts to prevent memory exhaustion.
const MAX_PENDING_PROMPTS: usize = 32;

/// Opens the user's browser to the permission page on the local dashboard,
/// then waits for the user to click a button. The HTTP POST handler sends
/// the response back via a oneshot channel.
///
/// Works from systemd user services because the node serves HTTP (already
/// running) and the shell page's JS handles browser notification + opening.
pub struct DashboardPrompter {
    pending: PendingPrompts,
}

impl DashboardPrompter {
    pub fn new(pending: PendingPrompts) -> Self {
        Self { pending }
    }
}

impl UserInputPrompter for DashboardPrompter {
    async fn prompt(
        &self,
        request: &UserInputRequest<'static>,
    ) -> Option<(usize, ClientResponse<'static>)> {
        if request.responses.is_empty() {
            tracing::warn!("RequestUserInput has no response options");
            return None;
        }

        if self.pending.len() >= MAX_PENDING_PROMPTS {
            tracing::warn!(
                max = MAX_PENDING_PROMPTS,
                "Too many pending permission prompts, auto-denying"
            );
            return None;
        }

        let message = parse_message(request);
        let labels = parse_button_labels(request);

        // Generate a 128-bit cryptographic nonce for the permission URL
        let nonce = generate_nonce();

        let (tx, rx) = oneshot::channel();

        self.pending.insert(
            nonce.clone(),
            PendingPrompt {
                message,
                labels,
                delegate_key: "Unknown".to_string(), // TODO: pass from executor context
                contract_id: "Unknown".to_string(),  // TODO: pass from executor context
                response_tx: tx,
            },
        );

        // Log at debug, not info -- nonce is the sole auth token for this prompt
        tracing::debug!(
            request_id = request.request_id,
            "Permission prompt created, waiting for user response via dashboard"
        );

        // Wait for user response with timeout
        let result = tokio::time::timeout(USER_INPUT_TIMEOUT, rx).await;

        // Clean up if still pending
        self.pending.remove(&nonce);

        match result {
            Ok(Ok(idx)) if idx < request.responses.len() => {
                let response = request.responses[idx].clone().into_owned();
                Some((idx, response))
            }
            Ok(Ok(_)) => {
                tracing::warn!(nonce = %nonce, "Invalid response index from dashboard");
                None
            }
            Ok(Err(_)) => {
                tracing::debug!(nonce = %nonce, "Permission prompt channel closed");
                None
            }
            Err(_) => {
                tracing::warn!(nonce = %nonce, "Permission prompt timed out after 60s");
                None
            }
        }
    }
}

/// Generate a 128-bit cryptographic hex nonce.
fn generate_nonce() -> String {
    use crate::config::GlobalRng;
    let a = GlobalRng::random_u64();
    let b = GlobalRng::random_u64();
    format!("{a:016x}{b:016x}")
}

/// Extract a displayable message from `NotificationMessage` bytes.
pub(crate) fn parse_message(request: &UserInputRequest<'_>) -> String {
    let bytes = request.message.bytes();
    let raw = if let Ok(json_str) = serde_json::from_slice::<String>(bytes) {
        json_str
    } else {
        String::from_utf8(bytes.to_vec())
            .unwrap_or_else(|_| "A delegate is requesting permission.".to_string())
    };
    raw.chars()
        .take(MAX_MESSAGE_LEN)
        .filter(|c| !c.is_control() || *c == '\n')
        .collect()
}

/// Extract button labels from `ClientResponse` bytes as sanitized UTF-8 strings.
pub(crate) fn parse_button_labels(request: &UserInputRequest<'_>) -> Vec<String> {
    request
        .responses
        .iter()
        .take(MAX_LABELS)
        .enumerate()
        .map(|(i, r)| {
            let label =
                String::from_utf8((**r).to_vec()).unwrap_or_else(|_| format!("Option {}", i + 1));
            label
                .chars()
                .take(MAX_LABEL_LEN)
                .filter(|c| !c.is_control())
                .collect()
        })
        .collect()
}

/// Auto-approves by returning the first response. For testing only.
pub struct AutoApprovePrompter;

impl UserInputPrompter for AutoApprovePrompter {
    async fn prompt(
        &self,
        request: &UserInputRequest<'static>,
    ) -> Option<(usize, ClientResponse<'static>)> {
        request
            .responses
            .first()
            .map(|r| (0, r.clone().into_owned()))
    }
}

/// Always denies (returns None). For headless environments where no display
/// is available (e.g., gateway servers, CI).
#[allow(dead_code)]
pub struct AutoDenyPrompter;

impl UserInputPrompter for AutoDenyPrompter {
    async fn prompt(
        &self,
        _request: &UserInputRequest<'static>,
    ) -> Option<(usize, ClientResponse<'static>)> {
        None
    }
}

#[cfg(test)]
pub(crate) fn make_test_request(message: &str, responses: Vec<&str>) -> UserInputRequest<'static> {
    use freenet_stdlib::prelude::NotificationMessage;

    let msg = NotificationMessage::try_from(&serde_json::Value::String(message.to_string()))
        .expect("valid JSON");
    UserInputRequest {
        request_id: 1,
        message: msg,
        responses: responses
            .into_iter()
            .map(|r| ClientResponse::new(r.as_bytes().to_vec()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auto_approve_returns_first_response() {
        let req = make_test_request("Allow this?", vec!["Allow", "Deny"]);
        let result = AutoApprovePrompter.prompt(&req).await;
        let (idx, response) = result.unwrap();
        assert_eq!(idx, 0);
        assert_eq!(&*response, b"Allow");
    }

    #[tokio::test]
    async fn test_auto_approve_empty_responses() {
        let req = make_test_request("Allow this?", vec![]);
        let result = AutoApprovePrompter.prompt(&req).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_auto_deny_always_returns_none() {
        let req = make_test_request("Allow this?", vec!["Allow", "Deny"]);
        let result = AutoDenyPrompter.prompt(&req).await;
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_button_labels() {
        let req = make_test_request("msg", vec!["Allow Once", "Always Allow", "Deny"]);
        let labels = parse_button_labels(&req);
        assert_eq!(labels, vec!["Allow Once", "Always Allow", "Deny"]);
    }

    #[test]
    fn test_parse_message_json_encoded() {
        let req = make_test_request("Hello world", vec![]);
        let msg = parse_message(&req);
        assert_eq!(msg, "Hello world");
    }

    #[test]
    fn test_parse_message_json_with_quotes() {
        use freenet_stdlib::prelude::NotificationMessage;
        let json_val = serde_json::Value::String("Test with \"quotes\"".to_string());
        let msg = NotificationMessage::try_from(&json_val).unwrap();
        let req = UserInputRequest {
            request_id: 1,
            message: msg,
            responses: vec![],
        };
        let parsed = parse_message(&req);
        assert_eq!(parsed, "Test with \"quotes\"");
    }

    #[tokio::test]
    async fn test_dashboard_prompter_max_pending() {
        let pending: PendingPrompts = Arc::new(DashMap::new());
        let prompter = DashboardPrompter::new(pending.clone());

        for i in 0..MAX_PENDING_PROMPTS {
            let (tx, _rx) = oneshot::channel();
            pending.insert(
                format!("nonce_{i}"),
                PendingPrompt {
                    message: "test".to_string(),
                    labels: vec!["OK".to_string()],
                    delegate_key: String::new(),
                    contract_id: String::new(),
                    response_tx: tx,
                },
            );
        }

        let req = make_test_request("Over limit", vec!["Allow"]);
        let result = prompter.prompt(&req).await;
        assert!(result.is_none());
    }

    #[test]
    fn test_nonce_is_32_hex_chars() {
        let nonce = generate_nonce();
        assert_eq!(nonce.len(), 32);
        assert!(nonce.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_parse_message_strips_control_chars() {
        use freenet_stdlib::prelude::NotificationMessage;
        let json_val = serde_json::Value::String("Hello\x00\x07world".to_string());
        let msg = NotificationMessage::try_from(&json_val).unwrap();
        let req = UserInputRequest {
            request_id: 1,
            message: msg,
            responses: vec![],
        };
        let parsed = parse_message(&req);
        assert_eq!(parsed, "Helloworld");
    }

    #[test]
    fn test_parse_button_labels_invalid_utf8() {
        use freenet_stdlib::prelude::NotificationMessage;
        let req = UserInputRequest {
            request_id: 1,
            message: NotificationMessage::try_from(&serde_json::Value::String("msg".to_string()))
                .unwrap(),
            responses: vec![
                ClientResponse::new(b"Valid".to_vec()),
                ClientResponse::new(vec![0xFF, 0xFE]),
            ],
        };
        let labels = parse_button_labels(&req);
        assert_eq!(labels, vec!["Valid", "Option 2"]);
    }

    #[test]
    fn test_parse_message_raw_utf8() {
        use freenet_stdlib::prelude::NotificationMessage;
        let raw_msg =
            NotificationMessage::try_from(&serde_json::Value::String("Raw message".to_string()))
                .unwrap();
        let req = UserInputRequest {
            request_id: 1,
            message: raw_msg,
            responses: vec![],
        };
        let msg = parse_message(&req);
        assert_eq!(msg, "Raw message");
    }

    #[tokio::test]
    async fn test_auto_approve_with_three_responses() {
        let req = make_test_request("Allow?", vec!["Allow Once", "Always Allow", "Deny"]);
        let result = AutoApprovePrompter.prompt(&req).await;
        let (idx, response) = result.unwrap();
        assert_eq!(idx, 0);
        assert_eq!(&*response, b"Allow Once");
    }

    #[tokio::test]
    async fn test_auto_deny_with_multiple_responses() {
        let req = make_test_request("Allow?", vec!["Allow Once", "Always Allow", "Deny"]);
        let result = AutoDenyPrompter.prompt(&req).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_dashboard_prompter_happy_path() {
        let pending: PendingPrompts = Arc::new(DashMap::new());
        let prompter = DashboardPrompter::new(pending.clone());

        let req = make_test_request("Allow signing?", vec!["Allow", "Deny"]);

        // Spawn the prompt in a task
        let pending_clone = pending.clone();
        let handle = tokio::spawn(async move { prompter.prompt(&req).await });

        // Wait briefly for the prompt to be inserted
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Find the nonce and respond
        let nonce = pending_clone
            .iter()
            .next()
            .expect("should have a pending prompt")
            .key()
            .clone();

        let (_, prompt) = pending_clone.remove(&nonce).unwrap();
        prompt.response_tx.send(0).unwrap();

        let result = handle.await.unwrap();
        let (idx, response) = result.unwrap();
        assert_eq!(idx, 0);
        assert_eq!(&*response, b"Allow");
    }
}
