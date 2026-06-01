//! CLI entry point: drive controllers against synthetic scenarios or OTLP
//! replay files.

use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use replay_harness::Replayer;
use replay_harness::controllers::{FixedRate, RfcDraft};
use replay_harness::scenarios::{self, Expectation};

#[derive(Parser)]
#[command(
    name = "replay-harness",
    about = "Offline shadow-RTT controller harness (#4074)"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List the built-in synthetic scenarios.
    Scenarios,
    /// List the built-in controllers.
    Controllers,
    /// Run one or all synthetic scenarios against the given controller.
    Synthetic {
        /// Scenario name, or `all` to run every scenario.
        scenario: String,
        /// Controller to drive (`fixed_rate` or `rfc_draft`).
        #[arg(long, default_value = "rfc_draft")]
        controller: String,
    },
    /// Replay an OTLP shadow_rtt_aggregate jsonl file through the given
    /// controller. (v1: stub — wire-up coming in #4314 follow-up.)
    Otlp {
        /// Path to the OTLP jsonl file.
        #[arg(long)]
        file: PathBuf,
        /// Controller to drive.
        #[arg(long, default_value = "rfc_draft")]
        controller: String,
    },
    // Note: the `compare <a> <b> <events>` subcommand listed in the
    // #4314 design is intentionally deferred. The Replayer already
    // produces a full DecisionLog for each run, so a future PR can
    // diff two runs without harness-side changes. Holding off on
    // deciding the diff/output format (text vs JSON vs side-by-side)
    // until we have a real Phase 2 candidate to compare against.
}

fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt().try_init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Scenarios => {
            for s in scenarios::all_scenarios() {
                println!(
                    "{:32} [{}] events={} run_for={:?}\n    {}",
                    s.name,
                    match s.expectation {
                        Expectation::NeverFires => "NEVER",
                        Expectation::FiresAtLeastOnce => "FIRES",
                        Expectation::Informational => "INFO",
                    },
                    s.events.len(),
                    s.run_for,
                    s.description,
                );
            }
        }
        Cmd::Controllers => {
            println!("fixed_rate   — baseline; never changes rate");
            println!("rfc_draft    — the #4074 sketched algorithm (downstep only)");
        }
        Cmd::Synthetic {
            scenario,
            controller,
        } => {
            let to_run: Vec<_> = if scenario == "all" {
                scenarios::all_scenarios()
            } else {
                match scenarios::find(&scenario) {
                    Some(s) => vec![s],
                    None => bail!("unknown scenario: {scenario}"),
                }
            };
            for s in to_run {
                run_synthetic(&s, &controller)?;
            }
        }
        Cmd::Otlp { file, controller } => {
            anyhow::ensure!(file.exists(), "OTLP file not found: {}", file.display());
            bail!(
                "OTLP replay for controller `{controller}` is not yet \
                 wired up; this v1 ships the synthetic-scenario harness only. \
                 See #4314 for the planned follow-up."
            );
        }
    }
    Ok(())
}

fn run_synthetic(s: &scenarios::Scenario, controller_name: &str) -> Result<()> {
    let report = match controller_name {
        "fixed_rate" => Replayer::new()
            .run_until(s.run_for)
            .run(s.events.clone().into_iter(), FixedRate),
        "rfc_draft" => Replayer::new()
            .run_until(s.run_for)
            .run(s.events.clone().into_iter(), RfcDraft::default()),
        other => bail!("unknown controller: {other}"),
    };

    // CLI output is neutral by design: it prints what the controller
    // actually did, plus the scenario's expectation for a *sane*
    // controller. Whether a specific controller's behaviour is correct
    // depends on its design philosophy — `FixedRate` (a no-op
    // baseline) never fires by definition, so it will always
    // "disagree" with `FiresAtLeastOnce` scenarios; that disagreement
    // is not a bug. Per-controller-per-scenario expected outcomes
    // live in `tests/scenarios_pin.rs` where they are pinned with
    // explicit knowledge of each controller's design.
    let scenario_expects = match s.expectation {
        Expectation::NeverFires => "scenario expects: no fire (sane controller)",
        Expectation::FiresAtLeastOnce => "scenario expects: at least one fire (sane controller)",
        Expectation::Informational => "scenario is informational",
    };
    println!(
        "{scenario}/{controller} — ticks={ticks} fires={fires} \
         final_rate_bps={final_rate} range_bps=[{min}..{max}]\n    ({expects})",
        scenario = s.name,
        controller = report.controller,
        ticks = report.ticks,
        fires = report.decisions_set,
        final_rate = report.final_rate_bps,
        min = report.min_rate_bps,
        max = report.max_rate_bps,
        expects = scenario_expects,
    );
    if !report.fired().is_empty() {
        for d in report.fired().iter().take(5) {
            if let replay_harness::RateDecision::Set { reason, .. } = &d.decision {
                println!(
                    "       fired at {:>6.1}s reason={reason} shared_infl={:?}",
                    d.at.as_secs_f64(),
                    d.shared_inflation,
                );
            }
        }
        if report.fired().len() > 5 {
            println!("       ... +{} more", report.fired().len() - 5);
        }
    }
    Ok(())
}
