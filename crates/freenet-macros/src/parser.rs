//! Parser for `#[freenet_test]` macro attributes

/// Parsed arguments for the `#[freenet_test]` attribute
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct FreenetTestArgs {
    /// All node labels
    pub nodes: Vec<String>,
    /// Which nodes are gateways (if not specified, first node is gateway)
    pub gateways: Option<Vec<String>>,
    /// Optional explicit node locations (same order as nodes)
    pub node_locations: Option<Vec<f64>>,
    /// Optional function path that returns node locations (same order as nodes)
    pub node_locations_fn: Option<syn::ExprPath>,
    /// Per-node configuration overrides
    pub node_configs: HashMap<String, NodeConfigOverride>,
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
        let mut node_locations = None;
        let mut node_locations_fn = None;
        let mut auto_connect_peers = true;
        let mut timeout_secs = 180;
        let mut startup_wait_secs = 15;
        let mut aggregate_events = AggregateEventsMode::OnFailure;
        let mut log_level = "freenet=debug,info".to_string();
        let mut tokio_flavor = TokioFlavor::CurrentThread;
        let mut tokio_worker_threads = None;
        let mut node_configs = HashMap::new();

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
                "node_configs" => {
                    let content;
                    syn::braced!(content in input);

                    while !content.is_empty() {
                        let label: syn::LitStr = content.parse()?;
                        let label_string = label.value();

                        content.parse::<syn::Token![:]>()?;
                        let cfg_content;
                        syn::braced!(cfg_content in content);

                        let mut override_cfg = NodeConfigOverride::default();

                        while !cfg_content.is_empty() {
                            let key: syn::Ident = cfg_content.parse()?;
                            cfg_content.parse::<syn::Token![:]>()?;

                            match key.to_string().as_str() {
                                "location" => {
                                    if override_cfg.location_expr.is_some() {
                                        return Err(syn::Error::new(
                                            key.span(),
                                            "Duplicate location entry for node config",
                                        ));
                                    }
                                    override_cfg.location_expr = Some(cfg_content.parse()?);
                                }
                                other => {
                                    return Err(syn::Error::new(
                                        key.span(),
                                        format!("Unknown node config option '{other}'"),
                                    ));
                                }
                            }

                            if cfg_content.peek(syn::Token![,]) {
                                cfg_content.parse::<syn::Token![,]>()?;
                            }
                        }

                        node_configs.insert(label_string, override_cfg);

                        if content.peek(syn::Token![,]) {
                            content.parse::<syn::Token![,]>()?;
                        }
                    }
                }
                "node_locations" => {
                    // Parse array literal of floats: [0.1, 0.5, 0.9]
                    let content;
                    syn::bracketed!(content in input);

                    let mut locations = Vec::new();
                    while !content.is_empty() {
                        let lit: syn::Lit = content.parse()?;
                        let value = match lit {
                            syn::Lit::Float(f) => f.base10_parse::<f64>()?,
                            syn::Lit::Int(i) => i.base10_parse::<f64>()?,
                            other => {
                                return Err(syn::Error::new(
                                    other.span(),
                                    "node_locations must be numeric literals",
                                ))
                            }
                        };

                        locations.push(value);

                        // Handle optional trailing comma
                        if content.peek(syn::Token![,]) {
                            content.parse::<syn::Token![,]>()?;
                        }
                    }

                    if locations.is_empty() {
                        return Err(syn::Error::new(
                            key.span(),
                            "node_locations array cannot be empty if specified",
                        ));
                    }

                    node_locations = Some(locations);
                }
                "node_locations_fn" => {
                    // Parse a path to a function returning Vec<f64>
                    let path: syn::ExprPath = input.parse()?;
                    node_locations_fn = Some(path);
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

        // Validate node_configs refer to known nodes
        for label in node_configs.keys() {
            if !nodes.contains(label) {
                return Err(input.error(format!(
                    "node_configs contains unknown node '{}'. All entries must reference nodes listed in 'nodes'.",
                    label
                )));
            }
        }

        // Validate node_locations if provided
        if let Some(ref locations) = node_locations {
            if locations.len() != nodes.len() {
                return Err(input.error(format!(
                    "node_locations length ({}) must match nodes length ({})",
                    locations.len(),
                    nodes.len()
                )));
            }
            for (i, &loc) in locations.iter().enumerate() {
                if !(0.0..=1.0).contains(&loc) {
                    return Err(input.error(format!(
                        "node_locations[{}] = {} is out of range. Values must be in [0.0, 1.0]",
                        i, loc
                    )));
                }
            }
        }

        // Only one of node_locations or node_locations_fn may be provided
        if node_locations.is_some() && node_locations_fn.is_some() {
            return Err(
                input.error("Specify only one of node_locations or node_locations_fn (not both)")
            );
        }

        Ok(FreenetTestArgs {
            nodes,
            gateways,
            node_locations,
            node_locations_fn,
            node_configs,
            auto_connect_peers,
            timeout_secs,
            startup_wait_secs,
            aggregate_events,
            log_level,
            tokio_flavor,
            tokio_worker_threads,
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
}
#[derive(Debug, Clone, Default)]
pub struct NodeConfigOverride {
    pub location_expr: Option<syn::Expr>,
}
