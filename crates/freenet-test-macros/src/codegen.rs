//! Code generation for `#[freenet_test]` macro

use crate::parser::{AggregateEventsMode, FreenetTestArgs};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, Result};

/// Generate the expanded test code from the macro attributes and test function
pub fn generate_test_code(args: FreenetTestArgs, input_fn: ItemFn) -> Result<TokenStream> {
    let test_fn_name = &input_fn.sig.ident;
    let inner_fn_name = format_ident!("{}_inner", test_fn_name);

    // Extract test body
    let test_body = &input_fn.block;

    // Generate node setup code
    let node_setup = generate_node_setup(&args);

    // Generate node startup tasks
    let node_tasks = generate_node_tasks(&args);

    // Generate TestContext creation
    let context_creation = generate_context_creation(&args);

    // Generate test coordination with select!
    let test_coordination = generate_test_coordination(&args, &inner_fn_name);

    // Generate failure reporting
    let failure_reporting = generate_failure_reporting(&args);

    // Build the complete generated code
    let log_level = &args.log_level;

    let generated = quote! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn #test_fn_name() -> freenet::test_utils::TestResult {
            use freenet::test_utils::{TestContext, TestLogger, NodeInfo};
            use std::time::{Duration, Instant};
            use tokio::select;
            use anyhow::anyhow;

            // 1. Setup TestLogger
            let _logger = TestLogger::new()
                .with_json()
                .with_level(#log_level)
                .init();

            tracing::info!("Starting test: {}", stringify!(#test_fn_name));

            // 2. Create node configurations
            #node_setup

            // 3. Start nodes with instrumentation
            #node_tasks

            // 4. Build TestContext
            #context_creation

            // 5. Run test with coordination
            let test_result = {
                #test_coordination
            };

            // 6. Handle failure reporting
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

    for (idx, _node_label) in args.nodes.iter().enumerate() {
        let config_var = format_ident!("config_{}", idx);
        let temp_var = format_ident!("temp_{}", idx);
        let is_gateway = idx == 0; // First node is gateway

        let gateway_setup = if is_gateway {
            quote! {
                let (#config_var, #temp_var) = {
                    let temp_dir = tempfile::tempdir()?;
                    let key = freenet::dev_tool::TransportKeypair::new();
                    let transport_keypair = temp_dir.path().join("private.pem");
                    key.save(&transport_keypair)?;
                    key.public().save(temp_dir.path().join("public.pem"))?;

                    let network_socket = std::net::TcpListener::bind("127.0.0.1:0")?;
                    let ws_socket = std::net::TcpListener::bind("127.0.0.1:0")?;
                    let network_port = network_socket.local_addr()?.port();
                    let ws_port = ws_socket.local_addr()?.port();

                    std::mem::drop(network_socket);
                    std::mem::drop(ws_socket);

                    let location: f64 = rand::Rng::random(&mut rand::thread_rng());

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
                            bandwidth_limit: None,
                            blocked_addresses: None,
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

                let gateway_info_0 = freenet::config::InlineGwConfig {
                    address: (std::net::Ipv4Addr::LOCALHOST, config_0.network_api.public_port.unwrap()).into(),
                    location: config_0.network_api.location,
                    public_key_path: temp_0.path().join("public.pem"),
                };
            }
        } else {
            quote! {
                let (#config_var, #temp_var) = {
                    let temp_dir = tempfile::tempdir()?;
                    let key = freenet::dev_tool::TransportKeypair::new();
                    let transport_keypair = temp_dir.path().join("private.pem");
                    key.save(&transport_keypair)?;
                    key.public().save(temp_dir.path().join("public.pem"))?;

                    let ws_socket = std::net::TcpListener::bind("127.0.0.1:0")?;
                    let ws_port = ws_socket.local_addr()?.port();
                    std::mem::drop(ws_socket);

                    let location: f64 = rand::Rng::random(&mut rand::thread_rng());

                    let config = freenet::config::ConfigArgs {
                        ws_api: freenet::config::WebsocketApiArgs {
                            address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                            ws_api_port: Some(ws_port),
                            token_ttl_seconds: None,
                            token_cleanup_interval_seconds: None,
                        },
                        network_api: freenet::config::NetworkArgs {
                            public_address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                            public_port: None,
                            is_gateway: false,
                            skip_load_from_network: true,
                            gateways: Some(vec![serde_json::to_string(&gateway_info_0)?]),
                            location: Some(location),
                            ignore_protocol_checking: true,
                            address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                            network_port: None,
                            bandwidth_limit: None,
                            blocked_addresses: None,
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
            }
        };

        setup_code.push(gateway_setup);
    }

    quote! {
        #(#setup_code)*
    }
}

/// Generate node startup tasks
fn generate_node_tasks(args: &FreenetTestArgs) -> TokenStream {
    let mut tasks = Vec::new();

    for (idx, node_label) in args.nodes.iter().enumerate() {
        let task_var = format_ident!("node_task_{}", idx);
        let config_var = format_ident!("config_{}", idx);

        tasks.push(quote! {
            let #task_var = {
                let config = #config_var;
                async move {
                    tracing::info!("Starting node: {}", #node_label);
                    let built_config = config.build().await?;
                    let node = freenet::local_node::NodeConfig::new(built_config.clone())
                        .await?
                        .build(freenet::server::serve_gateway(built_config.ws_api).await)
                        .await?;
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

/// Generate TestContext creation
fn generate_context_creation(args: &FreenetTestArgs) -> TokenStream {
    let mut node_infos = Vec::new();

    for (idx, node_label) in args.nodes.iter().enumerate() {
        let temp_var = format_ident!("temp_{}", idx);
        let config_var = format_ident!("config_{}", idx);
        let is_gateway = idx == 0;

        node_infos.push(quote! {
            NodeInfo {
                label: #node_label.to_string(),
                temp_dir_path: #temp_var.path().to_path_buf(),
                ws_port: #config_var.ws_api.ws_api_port.unwrap(),
                network_port: #config_var.network_api.public_port,
                is_gateway: #is_gateway,
                location: #config_var.network_api.location.unwrap(),
            }
        });
    }

    quote! {
        let mut ctx = TestContext::new(vec![
            #(#node_infos),*
        ]);
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
                Ok(Ok(val)) => Ok(val),
                Ok(Err(e)) => Err(e),
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
