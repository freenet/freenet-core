//! The nightly scenario: PUT via the gateway, GET everything back via a
//! fresh ephemeral peer, including previous runs' contracts (retention).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use freenet_stdlib::prelude::*;

use crate::manifest::{ContractRecord, Manifest, RunRecord};
use crate::report::{OpReport, Report};
use crate::{PutGetArgs, client, contracts, ephemeral};

pub async fn run(args: PutGetArgs) -> Result<bool> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before epoch")?
        .as_secs();
    let run_id = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let op_timeout = Duration::from_secs(args.op_timeout_secs);

    eprintln!("network-probe put-get: run {run_id}");
    let wasm = contracts::compile_contract_wasm(&args.contract_dir)?;
    let probe_contracts = contracts::build_probe_contracts(&wasm, &run_id, args.small_contracts)?;
    let mut manifest = Manifest::load(&args.manifest)?;
    let retention: Vec<(&str, &RunRecord)> = manifest.retention_targets(now);
    eprintln!(
        "retention targets this run: {:?}",
        retention
            .iter()
            .map(|(w, r)| (*w, r.run_id.as_str()))
            .collect::<Vec<_>>()
    );

    let mut report = Report::default();

    // ---- PUT phase, via the existing gateway ----
    let mut gw = client::connect(&args.gateway_ws, Duration::from_secs(15))
        .await
        .context("connecting to gateway ws api")?;
    let mut put_ok: Vec<&contracts::ProbeContract> = Vec::new();
    for c in &probe_contracts {
        let key = c.contract.key();
        match client::put(
            &mut gw,
            c.contract.clone(),
            c.state.clone(),
            &c.label,
            op_timeout,
        )
        .await
        {
            Ok(latency) => {
                report.push(OpReport {
                    op: "put",
                    age: "0h",
                    label: c.label.clone(),
                    key: key.to_string(),
                    ok: true,
                    latency_ms: latency.as_millis(),
                    size: c.state.as_ref().len(),
                    error: None,
                });
                put_ok.push(c);
            }
            Err(e) => report.push(OpReport {
                op: "put",
                age: "0h",
                label: c.label.clone(),
                key: key.to_string(),
                ok: false,
                latency_ms: op_timeout.as_millis(),
                size: c.state.as_ref().len(),
                error: Some(format!("{e:#}")),
            }),
        }
    }
    client::disconnect(&mut gw).await;

    eprintln!(
        "{}/{} PUTs ok; settling {}s before booting the ephemeral getter...",
        put_ok.len(),
        probe_contracts.len(),
        args.settle_secs
    );
    tokio::time::sleep(Duration::from_secs(args.settle_secs)).await;

    // ---- GET phase, via a fresh ephemeral peer ----
    let node = ephemeral::spawn_ephemeral(
        &args.freenet_bin,
        args.ephemeral_network_port,
        args.ephemeral_ws_port,
        &args.gateway_spec,
    )
    .await?;

    let join_timeout = Duration::from_secs(args.join_timeout_secs);
    let mut peer = match client::connect(&node.ws_base, join_timeout).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ephemeral node log tail:\n{}", node.log_tail());
            node.shutdown().await;
            return Err(e.context("connecting to ephemeral node ws api"));
        }
    };
    if let Err(e) = client::wait_for_ring_join(&mut peer, join_timeout).await {
        eprintln!("ephemeral node log tail:\n{}", node.log_tail());
        node.shutdown().await;
        return Err(e);
    }

    // Today's contracts: full byte-identity check against what we just PUT.
    for c in &put_ok {
        let key = c.contract.key();
        let outcome = client::get(&mut peer, *key.id(), &c.label, op_timeout)
            .await
            .and_then(|(contract, state, latency)| {
                anyhow::ensure!(
                    contract == c.contract,
                    "returned contract differs from what was PUT"
                );
                anyhow::ensure!(
                    state == c.state,
                    "returned state differs (got {} bytes, want {})",
                    state.as_ref().len(),
                    c.state.as_ref().len()
                );
                Ok(latency)
            });
        push_get_report(
            &mut report,
            "0h",
            &c.label,
            key.to_string(),
            c.state.as_ref().len(),
            outcome,
            op_timeout,
        );
    }

    // Previous runs: verify state via the recorded blake3 hash.
    for (window, run) in &retention {
        for rec in &run.contracts {
            let label = format!("{}/{}", run.run_id, rec.label);
            let outcome = client::get(&mut peer, rec.id, &label, op_timeout)
                .await
                .and_then(|(_contract, state, latency)| {
                    let hash = blake3::hash(state.as_ref()).to_hex().to_string();
                    anyhow::ensure!(
                        hash == rec.state_hash,
                        "state hash mismatch (got {hash}, want {})",
                        rec.state_hash
                    );
                    Ok(latency)
                });
            push_get_report(
                &mut report,
                window,
                &label,
                rec.id.to_string(),
                rec.size,
                outcome,
                op_timeout,
            );
        }
    }

    client::disconnect(&mut peer).await;
    node.shutdown().await;

    // Record only successfully-PUT contracts so retention runs never chase
    // contracts that were never stored.
    if !put_ok.is_empty() {
        manifest.record_run(
            RunRecord {
                run_id,
                timestamp: now,
                contracts: put_ok
                    .iter()
                    .map(|c| ContractRecord {
                        id: *c.contract.key().id(),
                        label: c.label.clone(),
                        size: c.state.as_ref().len(),
                        state_hash: blake3::hash(c.state.as_ref()).to_hex().to_string(),
                    })
                    .collect(),
            },
            now,
        );
        manifest.save(&args.manifest)?;
    }

    report.print_summary();
    Ok(report.all_ok())
}

fn push_get_report(
    report: &mut Report,
    age: &'static str,
    label: &str,
    key: String,
    size: usize,
    outcome: Result<Duration>,
    op_timeout: Duration,
) {
    match outcome {
        Ok(latency) => report.push(OpReport {
            op: "get",
            age,
            label: label.to_string(),
            key,
            ok: true,
            latency_ms: latency.as_millis(),
            size,
            error: None,
        }),
        Err(e) => report.push(OpReport {
            op: "get",
            age,
            label: label.to_string(),
            key,
            ok: false,
            latency_ms: op_timeout.as_millis(),
            size,
            error: Some(format!("{e:#}")),
        }),
    }
}
