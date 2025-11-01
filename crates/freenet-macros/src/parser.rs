//! Parser for `#[freenet_test]` macro attributes

/// Parsed arguments for the `#[freenet_test]` attribute
#[derive(Debug, Clone)]
pub struct FreenetTestArgs {
    /// All node labels
    pub nodes: Vec<String>,
    /// Which nodes are gateways (if not specified, first node is gateway)
    pub gateways: Option<Vec<String>>,
    /// Whether peers should auto-connect to gateways
    pub auto_connect_peers: bool,
    /// Test timeout in seconds
    pub timeout_secs: u64,
    /// Node startup wait in seconds
    pub startup_wait_secs: u64,
    /// When to aggregate events
    pub aggregate_events: AggregateEventsMode,
    /// Log level filter
    pub log_level: String,
    /// Tokio runtime flavor
    pub tokio_flavor: TokioFlavor,
    /// Tokio worker threads
    pub tokio_worker_threads: Option<usize>,
    /// Connectivity ratio between peers (0.0-1.0), controlling partial connectivity
    pub peer_connectivity_ratio: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokioFlavor {
    MultiThread,
    CurrentThread,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateEventsMode {
    OnFailure,
    Always,
    Never,
}

impl syn::parse::Parse for FreenetTestArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut nodes = None;
        let mut gateways = None;
        let mut auto_connect_peers = true;
        let mut timeout_secs = 180;
        let mut startup_wait_secs = 15;
        let mut aggregate_events = AggregateEventsMode::OnFailure;
        let mut log_level = "freenet=debug,info".to_string();
        let mut tokio_flavor = TokioFlavor::CurrentThread;
        let mut tokio_worker_threads = None;
        let mut peer_connectivity_ratio = None;

        // Parse key-value pairs
        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;

            match key.to_string().as_str() {
                "nodes" => {
                    // Parse array literal: ["gateway", "peer-1", ...]
                    let content;
                    syn::bracketed!(content in input);

                    let mut node_list = Vec::new();
                    while !content.is_empty() {
                        let lit: syn::LitStr = content.parse()?;
                        node_list.push(lit.value());

                        // Handle optional trailing comma
                        if content.peek(syn::Token![,]) {
                            content.parse::<syn::Token![,]>()?;
                        }
                    }

                    if node_list.is_empty() {
                        return Err(syn::Error::new(
                            key.span(),
                            "nodes array cannot be empty, must have at least one node",
                        ));
                    }

                    nodes = Some(node_list);
                }
                "gateways" => {
                    // Parse array literal: ["gateway-1", "gateway-2", ...]
                    let content;
                    syn::bracketed!(content in input);

                    let mut gateway_list = Vec::new();
                    while !content.is_empty() {
                        let lit: syn::LitStr = content.parse()?;
                        gateway_list.push(lit.value());

                        // Handle optional trailing comma
                        if content.peek(syn::Token![,]) {
                            content.parse::<syn::Token![,]>()?;
                        }
                    }

                    if gateway_list.is_empty() {
                        return Err(syn::Error::new(
                            key.span(),
                            "gateways array cannot be empty if specified",
                        ));
                    }

                    gateways = Some(gateway_list);
                }
                "timeout_secs" => {
                    let lit: syn::LitInt = input.parse()?;
                    timeout_secs = lit.base10_parse()?;
                }
                "startup_wait_secs" => {
                    let lit: syn::LitInt = input.parse()?;
                    startup_wait_secs = lit.base10_parse()?;
                }
                "aggregate_events" => {
                    let lit: syn::LitStr = input.parse()?;
                    aggregate_events = match lit.value().as_str() {
                        "on_failure" => AggregateEventsMode::OnFailure,
                        "always" => AggregateEventsMode::Always,
                        "never" => AggregateEventsMode::Never,
                        other => {
                            return Err(syn::Error::new(
                                lit.span(),
                                format!(
                                    "Invalid aggregate_events value: '{}'. \
                                     Must be 'on_failure', 'always', or 'never'",
                                    other
                                ),
                            ))
                        }
                    };
                }
                "log_level" => {
                    let lit: syn::LitStr = input.parse()?;
                    log_level = lit.value();
                }
                "tokio_flavor" => {
                    let lit: syn::LitStr = input.parse()?;
                    tokio_flavor = match lit.value().as_str() {
                        "multi_thread" => TokioFlavor::MultiThread,
                        "current_thread" => TokioFlavor::CurrentThread,
                        other => {
                            return Err(syn::Error::new(
                                lit.span(),
                                format!(
                                    "Invalid tokio_flavor value: '{}'. \
                                     Must be 'multi_thread' or 'current_thread'",
                                    other
                                ),
                            ))
                        }
                    };
                }
                "tokio_worker_threads" => {
                    let lit: syn::LitInt = input.parse()?;
                    tokio_worker_threads = Some(lit.base10_parse()?);
                }
                "auto_connect_peers" => {
                    let lit: syn::LitBool = input.parse()?;
                    auto_connect_peers = lit.value;
                }
                "peer_connectivity_ratio" => {
                    let lit: syn::LitFloat = input.parse()?;
                    let ratio: f64 = lit.base10_parse()?;
                    if ratio < 0.0 || ratio > 1.0 {
                        return Err(syn::Error::new(
                            lit.span(),
                            "peer_connectivity_ratio must be between 0.0 and 1.0",
                        ));
                    }
                    peer_connectivity_ratio = Some(ratio);
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("Unknown attribute '{}'", key),
                    ))
                }
            }

            // Handle optional trailing comma
            if input.peek(syn::Token![,]) {
                input.parse::<syn::Token![,]>()?;
            }
        }

        let nodes = nodes.ok_or_else(|| {
            input.error(
                "Required attribute 'nodes' is missing. Example: nodes = [\"gateway\", \"peer-1\"]",
            )
        })?;

        // Validate gateways if specified
        if let Some(ref gateway_list) = gateways {
            for gateway in gateway_list {
                if !nodes.contains(gateway) {
                    return Err(input.error(format!(
                        "Gateway '{}' is not in the nodes list. All gateways must be present in nodes.",
                        gateway
                    )));
                }
            }
        }

        Ok(FreenetTestArgs {
            nodes,
            gateways,
            auto_connect_peers,
            timeout_secs,
            startup_wait_secs,
            aggregate_events,
            log_level,
            tokio_flavor,
            tokio_worker_threads,
            peer_connectivity_ratio,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn test_parse_minimal() {
        let tokens = quote! {
            nodes = ["gateway", "peer-1"]
        };

        let args: FreenetTestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.nodes, vec!["gateway", "peer-1"]);
        assert_eq!(args.auto_connect_peers, true); // Verify default is true
        assert_eq!(args.timeout_secs, 180);
        assert_eq!(args.startup_wait_secs, 15);
        assert_eq!(args.aggregate_events, AggregateEventsMode::OnFailure);
    }

    #[test]
    fn test_parse_full() {
        let tokens = quote! {
            nodes = ["gateway", "peer-1", "peer-2"],
            timeout_secs = 240,
            startup_wait_secs = 20,
            aggregate_events = "always",
            log_level = "debug"
        };

        let args: FreenetTestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.nodes, vec!["gateway", "peer-1", "peer-2"]);
        assert_eq!(args.timeout_secs, 240);
        assert_eq!(args.startup_wait_secs, 20);
        assert_eq!(args.aggregate_events, AggregateEventsMode::Always);
        assert_eq!(args.log_level, "debug");
    }

    #[test]
    fn test_parse_invalid_aggregate_mode() {
        let tokens = quote! {
            nodes = ["gateway"],
            aggregate_events = "invalid"
        };

        let result: Result<FreenetTestArgs, _> = syn::parse2(tokens);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty_nodes() {
        let tokens = quote! {
            nodes = []
        };

        let result: Result<FreenetTestArgs, _> = syn::parse2(tokens);
        assert!(result.is_err());
    }

    #[test]
    fn test_auto_connect_peers_explicit_false() {
        let tokens = quote! {
            nodes = ["gateway", "peer-1"],
            auto_connect_peers = false
        };

        let args: FreenetTestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.nodes, vec!["gateway", "peer-1"]);
        assert_eq!(args.auto_connect_peers, false); // Verify explicit false works
    }

    #[test]
    fn test_auto_connect_peers_explicit_true() {
        let tokens = quote! {
            nodes = ["gateway", "peer-1"],
            auto_connect_peers = true
        };

        let args: FreenetTestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.nodes, vec!["gateway", "peer-1"]);
        assert_eq!(args.auto_connect_peers, true);
    }
}
