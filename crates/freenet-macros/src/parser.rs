//! Parser for `#[freenet_test]` macro attributes

/// Parsed arguments for the `#[freenet_test]` attribute
#[derive(Debug, Clone)]
pub struct FreenetTestArgs {
    /// Node labels (first is gateway)
    pub nodes: Vec<String>,
    /// Test timeout in seconds
    pub timeout_secs: u64,
    /// Node startup wait in seconds
    pub startup_wait_secs: u64,
    /// When to aggregate events
    pub aggregate_events: AggregateEventsMode,
    /// Log level filter
    pub log_level: String,
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
        let mut timeout_secs = 180;
        let mut startup_wait_secs = 15;
        let mut aggregate_events = AggregateEventsMode::OnFailure;
        let mut log_level = "freenet=debug,info".to_string();

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
            input.error("Required attribute 'nodes' is missing. Example: nodes = [\"gateway\", \"peer-1\"]")
        })?;

        Ok(FreenetTestArgs {
            nodes,
            timeout_secs,
            startup_wait_secs,
            aggregate_events,
            log_level,
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
