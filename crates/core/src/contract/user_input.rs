use std::time::Duration;

use freenet_stdlib::prelude::{ClientResponse, UserInputRequest};

/// Timeout for user input prompts. After this duration, the request is auto-denied.
pub(crate) const USER_INPUT_TIMEOUT: Duration = Duration::from_secs(60);

/// Abstracts user prompting for delegate `RequestUserInput` messages.
///
/// The runtime calls `prompt()` when a delegate needs user permission.
/// Implementations can show native dialogs, auto-respond for testing, etc.
pub trait UserInputPrompter: Send + Sync {
    /// Show a prompt to the user and return their chosen response.
    ///
    /// Returns `Some((index, response))` if the user chose a response,
    /// where `index` is the position in `request.responses` and `response`
    /// is the corresponding `ClientResponse`.
    ///
    /// Returns `None` on timeout, dismissal, or if prompting is unavailable
    /// (headless environment).
    fn prompt(
        &self,
        request: &UserInputRequest<'static>,
    ) -> impl std::future::Future<Output = Option<(usize, ClientResponse<'static>)>> + Send;
}

/// Shows a native webview dialog by spawning `freenet prompt` as a subprocess.
///
/// The subprocess gets its own main thread for the GUI event loop (required by
/// macOS Cocoa and Linux GTK). Communication is via CLI args (in) and stdout (out).
pub struct SubprocessPrompter;

impl SubprocessPrompter {
    /// Extract a displayable message from `NotificationMessage` bytes.
    ///
    /// The bytes may be JSON-encoded (via `TryFrom<&serde_json::Value>`) or raw UTF-8.
    /// Try JSON string first (unwraps quotes/escapes), fall back to raw UTF-8.
    fn parse_message(request: &UserInputRequest<'_>) -> String {
        let bytes = request.message.bytes();
        // Try to parse as a JSON string value (the stdlib's TryFrom<&Value> encodes as JSON)
        if let Ok(json_str) = serde_json::from_slice::<String>(bytes) {
            return json_str;
        }
        // Fall back to raw UTF-8
        String::from_utf8(bytes.to_vec())
            .unwrap_or_else(|_| "A delegate is requesting permission.".to_string())
    }

    fn parse_button_labels(request: &UserInputRequest<'_>) -> Vec<String> {
        request
            .responses
            .iter()
            .enumerate()
            .map(|(i, r)| {
                String::from_utf8((**r).to_vec()).unwrap_or_else(|_| format!("Option {}", i + 1))
            })
            .collect()
    }
}

impl UserInputPrompter for SubprocessPrompter {
    async fn prompt(
        &self,
        request: &UserInputRequest<'static>,
    ) -> Option<(usize, ClientResponse<'static>)> {
        let message = Self::parse_message(request);
        let labels = Self::parse_button_labels(request);

        if labels.is_empty() {
            tracing::warn!("RequestUserInput has no response options");
            return None;
        }

        let labels_json = match serde_json::to_string(&labels) {
            Ok(json) => json,
            Err(e) => {
                tracing::error!(error = %e, "Failed to serialize button labels");
                return None;
            }
        };

        let exe = match std::env::current_exe() {
            Ok(path) => path,
            Err(e) => {
                tracing::error!(error = %e, "Failed to determine current executable path");
                return None;
            }
        };

        let result = tokio::time::timeout(USER_INPUT_TIMEOUT, async {
            tokio::process::Command::new(&exe)
                .arg("prompt")
                .arg("--message")
                .arg(&message)
                .arg("--buttons")
                .arg(&labels_json)
                .arg("--timeout")
                .arg(USER_INPUT_TIMEOUT.as_secs().to_string())
                .output()
                .await
        })
        .await;

        match result {
            Ok(Ok(output)) => {
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                match stdout.parse::<i32>() {
                    Ok(idx) if idx >= 0 && (idx as usize) < request.responses.len() => {
                        let idx = idx as usize;
                        let response = request.responses[idx].clone().into_owned();
                        Some((idx, response))
                    }
                    Ok(_) => {
                        tracing::debug!("User dismissed or denied the prompt");
                        None
                    }
                    Err(e) => {
                        tracing::warn!(
                            stdout = %stdout,
                            error = %e,
                            "Failed to parse prompt subprocess output"
                        );
                        None
                    }
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "Failed to spawn prompt subprocess");
                None
            }
            Err(_) => {
                tracing::warn!("User input prompt timed out");
                None
            }
        }
    }
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
/// is available (e.g., gateway servers, CI). Will be wired in via configuration
/// as an alternative to `SubprocessPrompter`.
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

/// Helper to construct a `UserInputRequest` for testing.
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
        assert!(result.is_some());
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
        let labels = SubprocessPrompter::parse_button_labels(&req);
        assert_eq!(labels, vec!["Allow Once", "Always Allow", "Deny"]);
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
        let labels = SubprocessPrompter::parse_button_labels(&req);
        assert_eq!(labels, vec!["Valid", "Option 2"]);
    }

    #[test]
    fn test_parse_message_json_encoded() {
        // NotificationMessage bytes are JSON-encoded via TryFrom<&Value>
        let req = make_test_request("Hello world", vec![]);
        let msg = SubprocessPrompter::parse_message(&req);
        assert_eq!(msg, "Hello world");
    }

    #[test]
    fn test_parse_message_raw_utf8() {
        use freenet_stdlib::prelude::NotificationMessage;

        // Raw UTF-8 bytes (not JSON-encoded)
        let raw_msg =
            NotificationMessage::try_from(&serde_json::Value::String("Raw message".to_string()))
                .unwrap();
        let req = UserInputRequest {
            request_id: 1,
            message: raw_msg,
            responses: vec![],
        };
        let msg = SubprocessPrompter::parse_message(&req);
        assert_eq!(msg, "Raw message");
    }

    #[test]
    fn test_parse_message_json_with_quotes() {
        use freenet_stdlib::prelude::NotificationMessage;

        // JSON-encoded strings with quotes/escapes should be properly decoded.
        let json_val = serde_json::Value::String("Test with \"quotes\"".to_string());
        let msg = NotificationMessage::try_from(&json_val).unwrap();
        let req = UserInputRequest {
            request_id: 1,
            message: msg,
            responses: vec![],
        };
        let parsed = SubprocessPrompter::parse_message(&req);
        assert_eq!(parsed, "Test with \"quotes\"");
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
}
