use super::*;

/// Resolve the WebSocket URL from the config, either from `node_url` or constructed from address/port/mode.
pub(super) fn resolve_ws_url(cfg: &BaseConfig) -> anyhow::Result<String> {
    if let Some(node_url) = &cfg.node_url {
        if !node_url.starts_with("ws://") && !node_url.starts_with("wss://") {
            return Err(anyhow::anyhow!(
                "invalid --node-url: must start with ws:// or wss://"
            ));
        }
        Ok(node_url.clone())
    } else {
        let mode = cfg.mode;
        let address = cfg.address;
        let target = match mode {
            OperationMode::Local => {
                if !address.is_loopback() {
                    return Err(anyhow::anyhow!(
                        "invalid ip: {address}, expecting a loopback ip address in local mode"
                    ));
                }
                SocketAddr::new(address, cfg.port)
            }
            OperationMode::Network => SocketAddr::new(address, cfg.port),
        };
        Ok(format!(
            "ws://{target}/v1/contract/command?encodingProtocol=native"
        ))
    }
}

/// Sanitize a URL for logging by replacing path segments between host and `/v1/` with `***`.
fn sanitize_url_for_log(url: &str) -> String {
    if let Some(idx) = url.find("/v1/") {
        let scheme_end = url.find("://").map(|i| i + 3).unwrap_or(0);
        let host_end = url[scheme_end..].find('/').map(|i| i + scheme_end);
        if let Some(host_end) = host_end {
            if host_end < idx {
                return format!("{}/***/v1/{}", &url[..host_end], &url[idx + 4..]);
            }
        }
    }
    // No /v1/ path found or no secret segment; return as-is
    url.to_string()
}

pub(super) async fn start_api_client(cfg: BaseConfig) -> anyhow::Result<WebApi> {
    let url = resolve_ws_url(&cfg)?;

    let (stream, _) = tokio_tungstenite::connect_async(&url).await.map_err(|e| {
        let safe_url = sanitize_url_for_log(&url);
        tracing::error!(err=%e);
        anyhow::anyhow!("failed to connect to the host({safe_url}): {e}")
    })?;

    Ok(WebApi::start(stream))
}

pub(super) async fn execute_command(
    request: ClientRequest<'static>,
    api_client: &mut WebApi,
) -> anyhow::Result<()> {
    api_client.send(request).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn base_config_with_node_url(node_url: Option<String>) -> BaseConfig {
        BaseConfig {
            paths: Default::default(),
            mode: OperationMode::Local,
            port: 7509,
            address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            node_url,
        }
    }

    #[test]
    fn test_resolve_ws_url_with_node_url() {
        let url = "ws://remote:7520/secret/v1/contract/command?encodingProtocol=native";
        let cfg = base_config_with_node_url(Some(url.to_string()));
        assert_eq!(resolve_ws_url(&cfg).unwrap(), url);
    }

    #[test]
    fn test_resolve_ws_url_without_node_url() {
        let cfg = base_config_with_node_url(None);
        assert_eq!(
            resolve_ws_url(&cfg).unwrap(),
            "ws://127.0.0.1:7509/v1/contract/command?encodingProtocol=native"
        );
    }

    #[test]
    fn test_resolve_ws_url_rejects_non_ws_scheme() {
        let cfg = base_config_with_node_url(Some("http://example.com".to_string()));
        let err = resolve_ws_url(&cfg).unwrap_err();
        assert!(err.to_string().contains("must start with ws://"));
    }

    #[test]
    fn test_resolve_ws_url_local_mode_rejects_non_loopback() {
        let mut cfg = base_config_with_node_url(None);
        cfg.address = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let err = resolve_ws_url(&cfg).unwrap_err();
        assert!(err.to_string().contains("loopback"));
    }

    #[test]
    fn test_resolve_ws_url_network_mode() {
        let mut cfg = base_config_with_node_url(None);
        cfg.mode = OperationMode::Network;
        cfg.address = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        cfg.port = 8080;
        assert_eq!(
            resolve_ws_url(&cfg).unwrap(),
            "ws://10.0.0.1:8080/v1/contract/command?encodingProtocol=native"
        );
    }

    #[test]
    fn test_resolve_ws_url_accepts_wss() {
        let url = "wss://secure.example.com/v1/contract/command?encodingProtocol=native";
        let cfg = base_config_with_node_url(Some(url.to_string()));
        assert_eq!(resolve_ws_url(&cfg).unwrap(), url);
    }

    #[test]
    fn test_sanitize_url_hides_secret_path() {
        let url = "ws://host:7520/my-secret/v1/contract/command?encodingProtocol=native";
        assert_eq!(
            sanitize_url_for_log(url),
            "ws://host:7520/***/v1/contract/command?encodingProtocol=native"
        );
    }

    #[test]
    fn test_sanitize_url_no_secret() {
        let url = "ws://127.0.0.1:7509/v1/contract/command?encodingProtocol=native";
        assert_eq!(sanitize_url_for_log(url), url);
    }
}
