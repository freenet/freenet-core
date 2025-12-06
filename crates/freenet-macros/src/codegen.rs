//! Code generation for `#[freenet_test]` macro

use crate::parser::{AggregateEventsMode, FreenetTestArgs};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitInt, LitStr, Result};

/// Get the configured location for a node or fall back to randomness.
fn node_location(args: &FreenetTestArgs, idx: usize, label: &str) -> TokenStream {
    if let Some(config) = args.node_configs.get(label) {
        if let Some(expr) = &config.location_expr {
            let label_lit = LitStr::new(label, proc_macro2::Span::call_site());
            return quote! {{
                let value: f64 = (#expr);
                if !(0.0..=1.0).contains(&value) {
                    panic!(
                        "node '{}' location {} is out of range [0.0, 1.0]",
                        #label_lit, value
                    );
                }
                value
            }};
        }
    }

    if let Some(ref locations) = args.node_locations {
        let value = locations[idx];
        return quote! { #value };
    }

    if args.node_locations_fn.is_some() {
        let idx_lit = syn::Index::from(idx);
        return quote! {{
            if let Some(ref locs) = __node_locations {
                locs[#idx_lit]
            } else {
                rand::Rng::random(&mut rand::rng())
            }
        }};
    }

    quote! { rand::Rng::random(&mut rand::rng()) }
}

/// Generate node location initialization (literal list or function).
fn generate_node_locations_init(args: &FreenetTestArgs) -> TokenStream {
    let node_count = args.nodes.len();

    if let Some(ref fn_path) = args.node_locations_fn {
        quote! {
            let __node_locations: Option<Vec<f64>> = {
                let locs = #fn_path();
                if locs.len() != #node_count {
                    return Err(anyhow::anyhow!(
                        "node_locations_fn returned {} locations, expected {}",
                        locs.len(),
                        #node_count
                    ));
                }
                for (idx, loc) in locs.iter().enumerate() {
                    if !(0.0..=1.0).contains(loc) {
                        return Err(anyhow::anyhow!(
                            "node_locations_fn value at index {} is out of range: {} (must be in [0.0, 1.0])",
                            idx,
                            loc
                        ));
                    }
                }
                Some(locs)
            };
        }
    } else if let Some(ref locations) = args.node_locations {
        let values: Vec<_> = locations.iter().map(|loc| quote! { #loc }).collect();
        quote! {
            let __node_locations: Option<Vec<f64>> = Some(vec![#(#values),*]);
        }
    } else {
        quote! {
            let __node_locations: Option<Vec<f64>> = None;
        }
    }
}

/// Helper to determine if a node is a gateway
fn is_gateway(args: &FreenetTestArgs, node_label: &str, node_idx: usize) -> bool {
    if let Some(ref gateways) = args.gateways {
        // Explicit gateway list provided - check if this node is in it
        gateways.contains(&node_label.to_string())
    } else {
        // No explicit list - first node is gateway (backward compatibility)
        node_idx == 0
    }
}

/// Generate the expanded test code from the macro attributes and test function
pub fn generate_test_code(args: FreenetTestArgs, input_fn: ItemFn) -> Result<TokenStream> {
    let test_fn_name = &input_fn.sig.ident;
    let inner_fn_name = format_ident!("{}_inner", test_fn_name);
    let attrs = &input_fn.attrs;

    // Extract test body
    let test_body = &input_fn.block;

    // Generate node setup code
    let node_setup = generate_node_setup(&args);
    let node_locations_init = generate_node_locations_init(&args);

    // Extract values before configs are moved
    let value_extraction = generate_value_extraction(&args);

    // Build nodes and collect flush handles
    let node_builds = generate_node_builds(&args);

    // Generate TestContext creation with flush handles
    let context_creation = generate_context_creation_with_handles(&args);

    // Generate node startup tasks (runs already-built nodes)
    let node_tasks = generate_node_tasks(&args);

    // Generate test coordination with select!
    let test_coordination = generate_test_coordination(&args, &inner_fn_name);

    // Generate failure reporting
    let failure_reporting = generate_failure_reporting(&args);

    // Build the complete generated code
    let log_level = &args.log_level;

    // Generate tokio::test attribute with configuration
    let tokio_attr = generate_tokio_attr(&args);

    let generated = quote! {
        #tokio_attr
        #(#attrs)*
        async fn #test_fn_name() -> freenet::test_utils::TestResult {
            use freenet::test_utils::{TestContext, TestLogger, NodeInfo};
            use std::time::{Duration, Instant};
            use tokio::select;
            use anyhow::anyhow;

            // 1. Setup TestLogger
            let mut __test_logger = TestLogger::new().with_json().with_level(#log_level);
            if std::env::var_os("FREENET_TEST_LOG_DISABLE_JSON").is_some() {
                __test_logger = __test_logger.with_pretty();
            }
            let _logger = __test_logger.init();

            tracing::info!("Starting test: {}", stringify!(#test_fn_name));

            // 2. Create node configurations
            #node_locations_init
            #node_setup

            // 3. Extract values before configs are moved
            #value_extraction

            // 4. Build nodes and collect flush handles
            #node_builds

            // 5. Build TestContext with flush handles
            #context_creation

            // 6. Start node tasks (run already-built nodes)
            #node_tasks

            // 7. Run test with coordination
            // Note: Catching panics in async code while maintaining Send bounds
            // and avoiding ctx move issues is complex. For now, use Result-based
            // errors (bail!, ensure!) for best diagnostics.
            let test_result = {
                #test_coordination
            };

            // 8. Handle failure reporting
            #failure_reporting

            test_result
        }

        // User's test body as inner function
        async fn #inner_fn_name(ctx: &mut TestContext) -> freenet::test_utils::TestResult #test_body
    };

    Ok(generated)
}

/// Generate node configuration setup code
fn generate_node_setup(args: &FreenetTestArgs) -> TokenStream {
    let mut setup_code = Vec::new();
    let node_count = args.nodes.len();
    let node_count_lit = LitInt::new(&node_count.to_string(), proc_macro2::Span::call_site());

    // First, generate all keypairs upfront and verify uniqueness
    // This prevents rare RNG collisions from causing confusing test failures
    setup_code.push(quote! {
        // Generate all transport keypairs upfront
        let __keypairs: Vec<freenet::dev_tool::TransportKeypair> = (0..#node_count_lit)
            .map(|_| freenet::dev_tool::TransportKeypair::new())
            .collect();

        // Verify all public keys are unique (sanity check against RNG issues)
        // Note: We use the full public key for comparison (via its Hash impl),
        // NOT the Display string which only captures 12 bytes of the DER encoding.
        {
            let mut seen_keys = std::collections::HashSet::<freenet::dev_tool::TransportPublicKey>::new();
            for (idx, keypair) in __keypairs.iter().enumerate() {
                if !seen_keys.insert(keypair.public().clone()) {
                    return Err(anyhow::anyhow!(
                        "FATAL: Generated duplicate transport keypair for node {} (key: {}). \
                         This indicates an RNG issue in the test environment. \
                         Please report this to the Freenet developers.",
                        idx,
                        keypair.public()
                    ));
                }
            }
            tracing::debug!("Verified {} unique transport keypairs", #node_count_lit);
        }
    });

    for (idx, node_label) in args.nodes.iter().enumerate() {
        let config_var = format_ident!("config_{}", idx);
        let temp_var = format_ident!("temp_{}", idx);
        let is_gw = is_gateway(args, node_label, idx);
        let idx_lit = LitInt::new(&idx.to_string(), proc_macro2::Span::call_site());

        if is_gw {
            // Gateway node configuration
            let location_expr = node_location(args, idx, node_label);
            let network_port_var = format_ident!("network_port_{}", idx);
            let ws_port_var_local = format_ident!("ws_port_{}", idx);
            setup_code.push(quote! {
                let #network_port_var = freenet::test_utils::reserve_local_port()?;
                let #ws_port_var_local = freenet::test_utils::reserve_local_port()?;
                let (#config_var, #temp_var) = {
                    let temp_dir = tempfile::tempdir()?;
                    let key = &__keypairs[#idx_lit];
                    let transport_keypair = temp_dir.path().join("private.pem");
                    key.save(&transport_keypair)?;
                    key.public().save(temp_dir.path().join("public.pem"))?;

                    let network_port = #network_port_var;
                    let ws_port = #ws_port_var_local;

                    let location: f64 = #location_expr;

                    let config = freenet::config::ConfigArgs {
                        ws_api: freenet::config::WebsocketApiArgs {
                            address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                            ws_api_port: Some(ws_port),
                            token_ttl_seconds: None,
                            token_cleanup_interval_seconds: None,
                        },
                        network_api: freenet::config::NetworkArgs {
                            public_address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                            public_port: Some(network_port),
                            is_gateway: true,
                            skip_load_from_network: true,
                            gateways: Some(vec![]),
                            location: Some(location),
                            ignore_protocol_checking: true,
                            address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                            network_port: Some(network_port),
                            min_connections: None,
                            max_connections: None,
                            bandwidth_limit: None,
                            blocked_addresses: None,
                            transient_budget: None,
                            transient_ttl_secs: None,
                        },
                        config_paths: freenet::config::ConfigPathsArgs {
                            config_dir: Some(temp_dir.path().to_path_buf()),
                            data_dir: Some(temp_dir.path().to_path_buf()),
                        },
                        secrets: freenet::config::SecretArgs {
                            transport_keypair: Some(transport_keypair),
                            ..Default::default()
                        },
                        ..Default::default()
                    };

                    (config, temp_dir)
                };
            });
        }
    }

    // Second pass: Generate gateway info variables
    // We need these for both auto_connect_peers and backward compatibility
    for (idx, node_label) in args.nodes.iter().enumerate() {
        let is_gw = is_gateway(args, node_label, idx);
        if is_gw {
            let gateway_info_var = format_ident!("gateway_info_{}", idx);
            let config_var = format_ident!("config_{}", idx);
            let temp_var = format_ident!("temp_{}", idx);

            setup_code.push(quote! {
                let #gateway_info_var = freenet::config::InlineGwConfig {
                    address: (std::net::Ipv4Addr::LOCALHOST, #config_var.network_api.public_port.unwrap()).into(),
                    location: #config_var.network_api.location,
                    public_key_path: #temp_var.path().join("public.pem"),
                };
            });
        }
    }

    // Third pass: Generate peer configurations (non-gateway nodes)
    for (idx, node_label) in args.nodes.iter().enumerate() {
        let config_var = format_ident!("config_{}", idx);
        let temp_var = format_ident!("temp_{}", idx);
        let is_gw = is_gateway(args, node_label, idx);
        let idx_lit = LitInt::new(&idx.to_string(), proc_macro2::Span::call_site());

        if !is_gw {
            let location_expr = node_location(args, idx, node_label);
            // Collect gateway info variables to serialize
            let gateways_config = if args.auto_connect_peers {
                // Collect all gateway_info_X variables
                let gateway_vars: Vec<_> = args
                    .nodes
                    .iter()
                    .enumerate()
                    .filter(|(gw_idx, gw_label)| is_gateway(args, gw_label, *gw_idx))
                    .map(|(gw_idx, _)| format_ident!("gateway_info_{}", gw_idx))
                    .collect();

                quote! {
                    Some(vec![
                        #(serde_json::to_string(&#gateway_vars)?),*
                    ])
                }
            } else {
                // Backward compatibility: use first gateway only
                let first_gateway_idx = args
                    .nodes
                    .iter()
                    .enumerate()
                    .find(|(gw_idx, gw_label)| is_gateway(args, gw_label, *gw_idx))
                    .map(|(gw_idx, _)| gw_idx)
                    .expect("At least one gateway must exist");

                let first_gateway_var = format_ident!("gateway_info_{}", first_gateway_idx);

                quote! {
                    Some(vec![serde_json::to_string(&#first_gateway_var)?])
                }
            };

            // Peer node configuration
            let network_port_var = format_ident!("network_port_{}", idx);
            let ws_port_var_local = format_ident!("ws_port_{}", idx);
            setup_code.push(quote! {
                let #network_port_var = freenet::test_utils::reserve_local_port()?;
                let #ws_port_var_local = freenet::test_utils::reserve_local_port()?;
                let (#config_var, #temp_var) = {
                    let temp_dir = tempfile::tempdir()?;
                    let key = &__keypairs[#idx_lit];
                    let transport_keypair = temp_dir.path().join("private.pem");
                    key.save(&transport_keypair)?;
                    key.public().save(temp_dir.path().join("public.pem"))?;

                    let network_port = #network_port_var;
                    let ws_port = #ws_port_var_local;

                    let location: f64 = #location_expr;

                    let config = freenet::config::ConfigArgs {
                        ws_api: freenet::config::WebsocketApiArgs {
                            address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                            ws_api_port: Some(ws_port),
                            token_ttl_seconds: None,
                            token_cleanup_interval_seconds: None,
                        },
                        network_api: freenet::config::NetworkArgs {
                            public_address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                            public_port: Some(network_port),
                            is_gateway: false,
                            skip_load_from_network: true,
                            gateways: #gateways_config,
                            location: Some(location),
                            ignore_protocol_checking: true,
                            address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                            network_port: Some(network_port),
                            min_connections: None,
                            max_connections: None,
                            bandwidth_limit: None,
                            blocked_addresses: None,
                            transient_budget: None,
                            transient_ttl_secs: None,
                        },
                        config_paths: freenet::config::ConfigPathsArgs {
                            config_dir: Some(temp_dir.path().to_path_buf()),
                            data_dir: Some(temp_dir.path().to_path_buf()),
                        },
                        secrets: freenet::config::SecretArgs {
                            transport_keypair: Some(transport_keypair),
                            ..Default::default()
                        },
                        ..Default::default()
                    };

                    (config, temp_dir)
                };
            });
        }
    }

    quote! {
        #(#setup_code)*
    }
}

/// Generate node building and flush handle collection
fn generate_node_builds(args: &FreenetTestArgs) -> TokenStream {
    let mut builds = Vec::new();
    let node_count = args.nodes.len();

    // Encourage small test networks to converge to a mesh quickly.
    let connection_tuning = if node_count > 1 {
        let min_connections = (node_count - 1).max(1);
        let max_connections = std::cmp::max(min_connections + 2, min_connections * 2);

        let min_lit = LitInt::new(&min_connections.to_string(), proc_macro2::Span::call_site());
        let max_lit = LitInt::new(&max_connections.to_string(), proc_macro2::Span::call_site());

        quote! {
            let min_connections: usize = #min_lit;
            let max_connections: usize = #max_lit;
            node_config.min_number_of_connections(min_connections);
            node_config.max_number_of_connections(max_connections);
        }
    } else {
        quote! {}
    };

    for (idx, node_label) in args.nodes.iter().enumerate() {
        let node_var = format_ident!("node_{}", idx);
        let flush_handle_var = format_ident!("flush_handle_{}", idx);
        let config_var = format_ident!("config_{}", idx);
        let network_port_var = format_ident!("network_port_{}", idx);
        let ws_port_var = format_ident!("ws_port_{}", idx);

        builds.push(quote! {
            tracing::info!("Building node: {}", #node_label);
            // Release reserved ports just before binding to minimize race window
            freenet::test_utils::release_local_port(#network_port_var);
            freenet::test_utils::release_local_port(#ws_port_var);
            let built_config = #config_var.build().await?;
            let mut node_config = freenet::local_node::NodeConfig::new(built_config.clone()).await?;
            #connection_tuning
            let (#node_var, #flush_handle_var) = node_config
                .build_with_flush_handle(freenet::server::serve_gateway(built_config.ws_api).await)
                .await?;
        });
    }

    quote! {
        #(#builds)*
    }
}

/// Generate node startup tasks (after nodes are built)
fn generate_node_tasks(args: &FreenetTestArgs) -> TokenStream {
    let mut tasks = Vec::new();

    for (idx, node_label) in args.nodes.iter().enumerate() {
        let task_var = format_ident!("node_task_{}", idx);
        let node_var = format_ident!("node_{}", idx);

        tasks.push(quote! {
            let #task_var = {
                let node = #node_var;
                async move {
                    tracing::info!("Node running: {}", #node_label);
                    node.run().await
                }
                .instrument(tracing::info_span!("test_peer", test_node = #node_label))
                .boxed_local()
            };
        });
    }

    quote! {
        use futures::FutureExt;
        use tracing::Instrument;

        #(#tasks)*
    }
}

/// Extract values from configs before they're moved
/// Note: ws_port and network_port are already defined at top level by generate_config_setup,
/// so we only need to extract location here.
fn generate_value_extraction(args: &FreenetTestArgs) -> TokenStream {
    let mut value_extractions = Vec::new();

    for (idx, _node_label) in args.nodes.iter().enumerate() {
        let config_var = format_ident!("config_{}", idx);
        let location_var = format_ident!("location_{}", idx);

        value_extractions.push(quote! {
            // ws_port_X and network_port_X already exist from reserve_local_port calls
            let #location_var = #config_var.network_api.location.unwrap();
        });
    }

    quote! {
        #(#value_extractions)*
    }
}

/// Generate TestContext creation with flush handles
fn generate_context_creation_with_handles(args: &FreenetTestArgs) -> TokenStream {
    let mut node_infos = Vec::new();
    let mut flush_handle_pairs = Vec::new();

    for (idx, node_label) in args.nodes.iter().enumerate() {
        let temp_var = format_ident!("temp_{}", idx);
        let ws_port_var = format_ident!("ws_port_{}", idx);
        let network_port_var = format_ident!("network_port_{}", idx);
        let location_var = format_ident!("location_{}", idx);
        let flush_handle_var = format_ident!("flush_handle_{}", idx);
        let is_gw = is_gateway(args, node_label, idx);

        node_infos.push(quote! {
            NodeInfo {
                label: #node_label.to_string(),
                temp_dir_path: #temp_var.path().to_path_buf(),
                ws_port: #ws_port_var,
                network_port: Some(#network_port_var),
                is_gateway: #is_gw,
                location: #location_var,
            }
        });

        flush_handle_pairs.push(quote! {
            (#node_label.to_string(), #flush_handle_var)
        });
    }

    quote! {
        let mut ctx = TestContext::with_flush_handles(
            vec![#(#node_infos),*],
            vec![#(#flush_handle_pairs),*]
        );
    }
}

/// Generate test coordination code with select!
fn generate_test_coordination(args: &FreenetTestArgs, inner_fn_name: &syn::Ident) -> TokenStream {
    let timeout_secs = args.timeout_secs;
    let startup_wait_secs = args.startup_wait_secs;

    // Generate select! arms for each node
    let mut select_arms = Vec::new();
    for (idx, node_label) in args.nodes.iter().enumerate() {
        let task_var = format_ident!("node_task_{}", idx);
        select_arms.push(quote! {
            result = #task_var => {
                Err(anyhow!("Node '{}' exited unexpectedly: {:?}", #node_label, result))
            }
        });
    }

    // Add test arm
    select_arms.push(quote! {
        result = test_future => {
            match result {
                Ok(Ok(val)) => {
                    // Give event loggers time to flush their batches
                    // The EventRegister is cloned multiple times, so all senders need to be dropped
                    // before the record_logs task will exit and flush remaining events
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok(val)
                },
                Ok(Err(e)) => {
                    // Also flush on error
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Err(e)
                },
                Err(_) => Err(anyhow!("Test timed out after {} seconds", #timeout_secs)),
            }
        }
    });

    quote! {
        let test_future = tokio::time::timeout(
            Duration::from_secs(#timeout_secs),
            async {
                // Wait for nodes to start
                tracing::info!("Waiting {} seconds for nodes to start up", #startup_wait_secs);
                tokio::time::sleep(Duration::from_secs(#startup_wait_secs)).await;
                tracing::info!("Nodes should be ready, running test");

                // Run user's test
                #inner_fn_name(&mut ctx).await
            }
        );

        select! {
            #(#select_arms),*
        }
    }
}

/// Generate failure reporting code
fn generate_failure_reporting(args: &FreenetTestArgs) -> TokenStream {
    match args.aggregate_events {
        AggregateEventsMode::Always => quote! {
            // Always generate report
            let report = if let Err(ref e) = test_result {
                ctx.generate_failure_report(e).await
            } else {
                ctx.generate_success_summary().await
            };
            eprintln!("{}", report);
        },
        AggregateEventsMode::OnFailure => quote! {
            // Only on failure
            if let Err(ref e) = test_result {
                let report = ctx.generate_failure_report(e).await;
                eprintln!("{}", report);
            }
        },
        AggregateEventsMode::Never => quote! {
            // No reporting
        },
    }
}

/// Generate tokio::test attribute with configuration
fn generate_tokio_attr(args: &FreenetTestArgs) -> TokenStream {
    use crate::parser::TokioFlavor;

    let flavor = match args.tokio_flavor {
        TokioFlavor::MultiThread => quote! { "multi_thread" },
        TokioFlavor::CurrentThread => quote! { "current_thread" },
    };

    // worker_threads is only valid for multi_thread flavor
    match (args.tokio_flavor, args.tokio_worker_threads) {
        (TokioFlavor::MultiThread, Some(worker_threads)) => {
            quote! {
                #[tokio::test(flavor = #flavor, worker_threads = #worker_threads)]
            }
        }
        (TokioFlavor::CurrentThread, _) => {
            // current_thread doesn't support worker_threads
            quote! {
                #[tokio::test(flavor = #flavor)]
            }
        }
        (TokioFlavor::MultiThread, None) => {
            quote! {
                #[tokio::test(flavor = #flavor)]
            }
        }
    }
}
