use std::borrow::Cow;

use clap::Parser;
use freenet_stdlib::client_api::ClientRequest;

mod build;
mod commands;
mod config;
mod diagnostics;
mod inspect;
pub(crate) mod network_metrics_server;
mod new_package;
mod query;
mod testing;
mod util;
mod verify_state;
mod wasm_runtime;
mod website;

use crate::{
    build::build_package,
    commands::{get, put, subscribe, update},
    config::{Config, SubCommand},
    inspect::inspect,
    new_package::create_new_package,
    wasm_runtime::run_local_executor,
};

type CommandReceiver = tokio::sync::mpsc::Receiver<ClientRequest<'static>>;
type CommandSender = tokio::sync::mpsc::Sender<ClientRequest<'static>>;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Configuration error: {0}")]
    MissConfiguration(Cow<'static, str>),
    #[error("Command failed: {0}")]
    CommandFailed(&'static str),
}

/// Process exit code for a response timeout (see [`commands::ResponseTimeout`]
/// and issue #4102). Distinct from the generic failure code (1) so release
/// automation can treat a timeout as "verify out-of-band before failing"
/// rather than a hard failure.
///
/// Deliberately NOT `2`: clap emits exit code `2` for CLI usage errors
/// (unknown flag, missing argument, etc.) from `Config::parse()` before any
/// of our code runs. Reusing `2` would let a usage error — where nothing was
/// ever sent to the node — masquerade as "submitted, may have succeeded",
/// which is exactly the misclassification this distinct code exists to
/// prevent. `3` is the first free code above clap's reserved range.
const EXIT_RESPONSE_TIMEOUT: i32 = 3;

/// Map a top-level error to the process exit code. A [`commands::ResponseTimeout`]
/// (possibly wrapped by `anyhow`) maps to [`EXIT_RESPONSE_TIMEOUT`]; everything
/// else maps to the generic failure code `1`.
fn exit_code_for_error(err: &anyhow::Error) -> i32 {
    if err.downcast_ref::<commands::ResponseTimeout>().is_some() {
        EXIT_RESPONSE_TIMEOUT
    } else {
        1
    }
}

fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    if !config.sub_command.is_child() {
        freenet::config::set_logger(None, None, config.additional.paths.log_dir.as_deref());
    }

    // Test subcommand uses Turmoil which requires running outside of any tokio runtime
    if matches!(config.sub_command, SubCommand::Test(_)) {
        if let SubCommand::Test(test_config) = config.sub_command {
            return testing::test_framework(test_config).map_err(|e| anyhow::format_err!(e));
        }
    }

    // All other commands use regular tokio runtime
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = tokio_rt.block_on(async move {
        let cwd = std::env::current_dir()?;
        let r = match config.sub_command {
            SubCommand::WasmRuntime(local_node_config) => {
                run_local_executor(local_node_config).await
            }
            SubCommand::Build(build_tool_config) => build_package(build_tool_config, &cwd),
            SubCommand::Inspect(inspect_config) => inspect(inspect_config),
            SubCommand::Init(init_pckg_config) => create_new_package(init_pckg_config),
            SubCommand::New(new_pckg_config) => create_new_package(new_pckg_config.into()),
            SubCommand::Publish(publish_config) => put(publish_config, config.additional).await,
            SubCommand::Execute(cmd_config) => match cmd_config.command {
                config::NodeCommand::Put(put_config) => put(put_config, config.additional).await,
                config::NodeCommand::Get(get_config) => get(get_config, config.additional).await,
                config::NodeCommand::Update(update_config) => {
                    update(update_config, config.additional).await
                }
                config::NodeCommand::Subscribe(subscribe_config) => {
                    subscribe(subscribe_config, config.additional).await
                }
                config::NodeCommand::GetContractId(get_contract_id_config) => {
                    commands::get_contract_id(get_contract_id_config).await
                }
            },
            SubCommand::Test(_) => unreachable!("Test handled above"),
            SubCommand::NetworkMetricsServer(server_config) => {
                let (server, _) = crate::network_metrics_server::start_server(&server_config).await;
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = server => {}
                }
                Ok(())
            }
            SubCommand::Query {} => {
                query::query(config.additional).await?;
                Ok(())
            }
            SubCommand::Diagnostics { contract_keys } => {
                diagnostics::diagnostics(config.additional, contract_keys).await?;
                Ok(())
            }
            SubCommand::GetContractId(get_contract_id_config) => {
                commands::get_contract_id(get_contract_id_config).await
            }
            SubCommand::VerifyState(verify_config) => {
                verify_state::verify_state(verify_config).await
            }
            SubCommand::Website { command } => match command {
                website::WebsiteCommand::Init { name } => website::init(name),
                website::WebsiteCommand::Publish {
                    directory,
                    key,
                    contract_wasm,
                    timeout,
                } => {
                    website::publish(directory, key, contract_wasm, timeout, config.additional)
                        .await
                }
                website::WebsiteCommand::Update {
                    directory,
                    key,
                    contract_wasm,
                    timeout,
                } => {
                    website::update(directory, key, contract_wasm, timeout, config.additional).await
                }
                website::WebsiteCommand::List => website::list(),
            },
        };
        // todo: make all commands return concrete `thiserror` compatible errors so we can use anyhow
        // Preserve the concrete `ResponseTimeout` type so main() can map it to a
        // distinct exit code (#4102); re-wrap every other error as before.
        r.map_err(|e| {
            if e.downcast_ref::<commands::ResponseTimeout>().is_some() {
                e
            } else {
                anyhow::format_err!(e)
            }
        })
    });

    // A response timeout exits with a distinct code (#4102) so release scripts
    // can distinguish "timed out, may have succeeded" from a hard failure.
    if let Err(err) = &result {
        let code = exit_code_for_error(err);
        if code != 1 {
            eprintln!("Error: {err:#}");
            std::process::exit(code);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{EXIT_RESPONSE_TIMEOUT, exit_code_for_error};
    use crate::commands::ResponseTimeout;
    use std::time::Duration;

    /// #4102: a response timeout maps to a distinct exit code so release
    /// automation can verify out-of-band before treating it as a hard failure.
    #[test]
    fn response_timeout_maps_to_distinct_exit_code() {
        let err: anyhow::Error = ResponseTimeout {
            target: "contract ABC".to_string(),
            timeout: Duration::from_secs(300),
        }
        .into();
        assert_eq!(exit_code_for_error(&err), EXIT_RESPONSE_TIMEOUT);
        assert_eq!(EXIT_RESPONSE_TIMEOUT, 3);
    }

    /// #4102: the timeout exit code must NOT collide with clap's usage-error
    /// code (2). A release script treating exit 2 as "submitted, may have
    /// succeeded" would otherwise misclassify a CLI usage error (unknown flag,
    /// e.g. passing `--timeout` to an older fdev) where nothing was ever sent.
    #[test]
    fn timeout_exit_code_does_not_collide_with_clap_usage_error() {
        const CLAP_USAGE_ERROR: i32 = 2;
        assert_ne!(
            EXIT_RESPONSE_TIMEOUT, CLAP_USAGE_ERROR,
            "response-timeout exit code must not reuse clap's usage-error code"
        );
    }

    /// Any other error keeps the generic failure code (1).
    #[test]
    fn other_errors_map_to_generic_failure_code() {
        let err = anyhow::anyhow!("boom");
        assert_eq!(exit_code_for_error(&err), 1);
    }
}
