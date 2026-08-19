use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum InboundAppMessage {
    SendToDelegate {
        target_key_bytes: Vec<u8>,
        target_code_hash: Vec<u8>,
        payload: Vec<u8>,
    },
    Ping {
        data: Vec<u8>,
    },
    /// Ask this delegate whether it holds the secret deposited by a previous
    /// `SECRET_PREFIX` delegate message, and whether it matches `expected`.
    ///
    /// The comparison happens INSIDE the delegate and only a boolean leaves it,
    /// so this query cannot itself become the leak it exists to rule out.
    VerifySecret {
        expected: Vec<u8>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OutboundAppMessage {
    MessageSent,
    DelegateMessageReceived {
        sender_key_bytes: Vec<u8>,
        payload: Vec<u8>,
        /// Runtime-attested caller key extracted from the `origin` parameter
        /// passed to `process()`. `Some` when origin was
        /// `MessageOrigin::Delegate(k)` (issue #3860); `None` otherwise.
        origin_delegate_key_bytes: Option<Vec<u8>>,
    },
    PingResponse {
        data: Vec<u8>,
    },
    /// Emitted after a `SECRET_PREFIX` delegate message is stored.
    ///
    /// Deliberately carries NO payload bytes — only a length and the attested
    /// sender. This is the "output discipline" a receiving delegate must
    /// maintain: its output goes to whoever drove the SENDER, so anything it
    /// puts here is client-visible.
    SecretStored {
        byte_count: u64,
        sender_key_bytes: Vec<u8>,
        origin_delegate_key_bytes: Option<Vec<u8>>,
    },
    /// Answer to `VerifySecret`. Booleans only, never the stored bytes.
    SecretVerified {
        present: bool,
        matches: bool,
    },
}

/// A delegate message whose payload begins with this prefix is treated as a
/// secret DEPOSIT: the receiver stores the remainder via `set_secret` and
/// reports only a byte count. Any other payload keeps the pre-existing
/// echo-it-back behaviour, which the `..._e2e` test asserts and which is the
/// negative control proving this harness can observe a leak at all.
pub const SECRET_PREFIX: &[u8] = b"SECRET:";

/// Where a deposited secret lands.
pub const DEPOSITED_SECRET_KEY: &[u8] = b"deposited-secret";

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        ctx: &mut DelegateCtx,
        _params: Parameters<'static>,
        origin: Option<MessageOrigin>,
        messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match messages {
            InboundDelegateMsg::ApplicationMessage(incoming_app) => {
                let message: InboundAppMessage =
                    bincode::deserialize(incoming_app.payload.as_slice())
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                match message {
                    InboundAppMessage::SendToDelegate {
                        target_key_bytes,
                        target_code_hash,
                        payload,
                    } => {
                        let key_arr: [u8; 32] = target_key_bytes
                            .try_into()
                            .map_err(|_| DelegateError::Other("key must be 32 bytes".into()))?;
                        let hash_arr: [u8; 32] = target_code_hash
                            .try_into()
                            .map_err(|_| DelegateError::Other("hash must be 32 bytes".into()))?;
                        let target = DelegateKey::new(key_arr, CodeHash::new(hash_arr));

                        // Sender is a placeholder; the runtime will overwrite it
                        // with the actual sender key (sender attestation).
                        let sender = DelegateKey::new([0u8; 32], CodeHash::new([0u8; 32]));
                        let msg = DelegateMessage::new(target, sender, payload);

                        let response_payload = bincode::serialize(&OutboundAppMessage::MessageSent)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response = ApplicationMessage::new(response_payload).processed(true);

                        Ok(vec![
                            OutboundDelegateMsg::SendDelegateMessage(msg),
                            OutboundDelegateMsg::ApplicationMessage(response),
                        ])
                    }
                    InboundAppMessage::Ping { data } => {
                        let response_payload =
                            bincode::serialize(&OutboundAppMessage::PingResponse { data })
                                .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response = ApplicationMessage::new(response_payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }
                    InboundAppMessage::VerifySecret { expected } => {
                        // Compare INSIDE the delegate and emit only booleans, so
                        // this query cannot become the leak it exists to rule out.
                        let stored = ctx.get_secret(DEPOSITED_SECRET_KEY);
                        let present = stored.is_some();
                        let matches = stored.as_deref() == Some(expected.as_slice());
                        let response_payload =
                            bincode::serialize(&OutboundAppMessage::SecretVerified {
                                present,
                                matches,
                            })
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response = ApplicationMessage::new(response_payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }
                }
            }
            InboundDelegateMsg::DelegateMessage(msg) => {
                let sender_key_bytes = msg.sender.bytes().to_vec();
                // Runtime-attested caller key. Computed before either branch so
                // the deposit path reports it too.
                let attested_origin = match &origin {
                    Some(MessageOrigin::Delegate(k)) => Some(k.bytes().to_vec()),
                    Some(MessageOrigin::WebApp(_)) | None => None,
                    Some(_) => None,
                };

                // SECRET DEPOSIT PATH. Store the payload and report a COUNT ONLY.
                //
                // This is the shape a real succession receiver must use. The
                // sibling branch below echoes the payload instead, and
                // `run_delegate_messaging_e2e` asserts that echo reaches the
                // driving CLIENT — so the two branches together demonstrate that
                // privacy across a delegate hop is the receiver's own output
                // discipline and nothing the runtime enforces.
                if let Some(secret) = msg.payload.strip_prefix(SECRET_PREFIX) {
                    let byte_count = secret.len() as u64;
                    ctx.set_secret(DEPOSITED_SECRET_KEY, secret);
                    let response_payload = bincode::serialize(&OutboundAppMessage::SecretStored {
                        byte_count,
                        sender_key_bytes,
                        origin_delegate_key_bytes: attested_origin,
                    })
                    .map_err(|err| DelegateError::Other(format!("{err}")))?;
                    let response = ApplicationMessage::new(response_payload).processed(true);
                    return Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)]);
                }
                // Echo the runtime-attested origin so the test can assert that
                // `MessageOrigin::Delegate(caller_key)` reaches the receiver
                // (issue #3860). Match exhaustively so a future MessageOrigin
                // variant isn't silently dropped.
                let origin_delegate_key_bytes = attested_origin;
                let response_payload =
                    bincode::serialize(&OutboundAppMessage::DelegateMessageReceived {
                        sender_key_bytes,
                        payload: msg.payload,
                        origin_delegate_key_bytes,
                    })
                    .map_err(|err| DelegateError::Other(format!("{err}")))?;

                let response = ApplicationMessage::new(response_payload).processed(true);
                Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
            }
            _ => Err(DelegateError::Other(
                "Unexpected inbound message".to_string(),
            )),
        }
    }
}
