#!/usr/bin/env python3
"""The two falsification campaigns for the delegate-park work (#5554, #5606).

WHY THIS IS COMMITTED AND NOT SCRATCH. The previous run of these campaigns
lived in a job's temporary directory, so the harness survived and the CASES did
not. Re-running them after a rebase meant reconstructing every mutation from
the `FALSIFY` rustdoc lines, and a reconstructed case list is a different
experiment from the one whose table was quoted. Reviews are per code content:
if the code moves, these have to run again, and a campaign nobody can re-run is
a claim rather than a result.

TWO CAMPAIGNS, deliberately separate:

  * `bytes`  -- the park byte accounting. Every term of `outbound_bytes`,
    `inbound_bytes`, `task_bytes`, `request_bytes` and the two helpers, zeroed
    one at a time, plus the two `#[non_exhaustive]` wildcards routed back to a
    free charge. The whole point of #5606's first half is that these terms were
    deletable with a 5486-test suite staying green.

  * `guards` -- the sweep budget, the delivery sites, the context preservation,
    the fetch allowance, the ceiling guard and the chokepoint scan. These are
    guards ABOUT other code, so a green guard proves nothing until it has been
    watched to fail.

Each case names the mutation, the tests it should turn red, and the verdict it
is expected to produce. A case whose verdict differs from `expect` is printed
with `!!` and makes the campaign exit non-zero. Two cases expect GREEN: they
are documented LIMITS of a source-scrape pin, and a campaign that only records
kills cannot tell a limit from an oversight.

Usage:
    PARK_HARNESS_WORKTREE=/path/to/worktree python3 scripts/park_mutation_campaign.py bytes
    PARK_HARNESS_WORKTREE=/path/to/worktree python3 scripts/park_mutation_campaign.py guards
"""
import subprocess
import sys

from park_mutation_harness import Tree, WORKTREE, campaign

PARK = "crates/core/src/contract/delegate_park.rs"
CONTRACT = "crates/core/src/contract.rs"
EXECUTOR = "crates/core/src/contract/executor.rs"

FILES = [PARK, CONTRACT, EXECUTOR]

# Every byte-accounting test, run for every byte-accounting case. Compilation
# dominates the cost of a case, so narrowing the filter per case would buy
# nothing and would let a term be zeroed with only its own test watching.
BYTE_TESTS = [
    "every_outbound_variant_charges_every_payload_it_retains",
    "every_inbound_variant_charges_every_payload_it_retains",
    "every_task_bytes_term_is_charged",
    "every_request_bytes_term_is_charged",
    "empty_messages_are_charged_for_their_slots",
    "a_queued_registration_charges_its_parameters_not_just_its_wasm",
    "task_bytes_charges_the_upsert_context_and_reserves_its_fetch",
    "message_contexts_are_charged_not_just_payloads",
    "every_context_carrying_variant_is_charged",
    "the_byte_cap_charges_retained_payloads_not_just_the_continuation",
    "an_unmeasurable_variant_cannot_be_admitted_at_any_budget",
]

BYTES_CASES = [
    (
        "outbound SendDelegateMessage payload zeroed",
        PARK,
        "OutboundDelegateMsg::SendDelegateMessage(m) => {\n"
        "            ByteCount::new(m.payload.len()) + ctx_len(&m.context)\n"
        "        }",
        "OutboundDelegateMsg::SendDelegateMessage(m) => {\n"
        "            ByteCount::new(0) + ctx_len(&m.context)\n"
        "        }",
        BYTE_TESTS,
        "RED",
    ),
    (
        "outbound PutContractRequest state zeroed",
        PARK,
        "ByteCount::new(r.state.as_ref().len())\n"
        "                + contract_container_bytes(&r.contract)",
        "ByteCount::new(0)\n                + contract_container_bytes(&r.contract)",
        BYTE_TESTS,
        "RED",
    ),
    (
        "outbound PutContractRequest contract zeroed",
        PARK,
        "+ contract_container_bytes(&r.contract)",
        "+ ByteCount::default()",
        BYTE_TESTS,
        "RED",
    ),
    (
        "outbound PutContractRequest related_contracts zeroed",
        PARK,
        "+ related_contracts_bytes(&r.related_contracts)",
        "+ ByteCount::default()",
        BYTE_TESTS,
        "RED",
    ),
    (
        "outbound UpdateContractRequest update zeroed",
        PARK,
        "update_data_bytes(&r.update) + ctx_len(&r.context)",
        "ByteCount::default() + ctx_len(&r.context)",
        BYTE_TESTS,
        "RED",
    ),
    (
        "outbound ContextUpdated zeroed",
        PARK,
        "OutboundDelegateMsg::ContextUpdated(c) => ctx_len(c),",
        "OutboundDelegateMsg::ContextUpdated(_c) => ByteCount::default(),",
        BYTE_TESTS,
        "RED",
    ),
    (
        "inbound UserResponse context zeroed",
        PARK,
        "InboundDelegateMsg::UserResponse(r) => {\n"
        "            ByteCount::new(r.response.len()) + ctx_len(&r.context)\n"
        "        }",
        "InboundDelegateMsg::UserResponse(r) => {\n"
        "            ByteCount::new(r.response.len()) + ByteCount::default()\n"
        "        }",
        BYTE_TESTS,
        "RED",
    ),
    (
        "inbound GetContractResponse state zeroed",
        PARK,
        "ByteCount::new(r.state.as_ref().map_or(0, |s| s.as_ref().len())) + ctx_len(&r.context)",
        "ByteCount::new(0) + ctx_len(&r.context)",
        BYTE_TESTS,
        "RED",
    ),
    (
        "inbound #[non_exhaustive] wildcard charged FREE instead of maximal",
        PARK,
        'other => unmeasurable("InboundDelegateMsg", std::mem::discriminant(other)),',
        "_other => ByteCount::default(),",
        BYTE_TESTS,
        "RED",
    ),
    (
        "request_bytes #[non_exhaustive] wildcard charged FREE instead of maximal",
        PARK,
        'other => unmeasurable("DelegateRequest", std::mem::discriminant(other)),',
        "_other => ByteCount::default(),",
        BYTE_TESTS,
        "RED",
    ),
    (
        "request_bytes ApplicationMessages params zeroed",
        PARK,
        "+ ByteCount::new(params.as_ref().len())",
        "+ ByteCount::default()",
        BYTE_TESTS,
        "RED",
    ),
    (
        "delegate_container_bytes params zeroed (the WASM still charged)",
        PARK,
        "ByteCount::new(d.code().as_ref().len()) + ByteCount::new(d.params().as_ref().len())",
        "ByteCount::new(d.code().as_ref().len()) + ByteCount::default()",
        BYTE_TESTS,
        "RED",
    ),
    (
        "task_bytes upsert context zeroed",
        PARK,
        "let context = ctx_len(&u.context) + ctx_len(&u.context);",
        "let context = ByteCount::default();",
        BYTE_TESTS,
        "RED",
    ),
    (
        "task_bytes fetch reserve dropped",
        PARK,
        "            let fetch_reserve = if u.missing.is_empty() {\n"
        "                ByteCount::default()\n"
        "            } else {\n"
        "                fetch_allowance\n"
        "            };",
        "            let fetch_reserve = ByteCount::default();\n"
        "            let _ = fetch_allowance;",
        BYTE_TESTS,
        "RED",
    ),
    (
        "continuation_bytes params dropped",
        PARK,
        "+ ByteCount::new(continuation.params.as_ref().len())",
        "+ ByteCount::default()",
        BYTE_TESTS,
        "RED",
    ),
    (
        "ELEMENT_OVERHEAD_BYTES set to 0",
        PARK,
        "const ELEMENT_OVERHEAD_BYTES: ByteCount = ByteCount::new(256);",
        "const ELEMENT_OVERHEAD_BYTES: ByteCount = ByteCount::new(0);",
        BYTE_TESTS,
        "RED",
    ),
]

GUARDS_CASES = [
    (
        "TTL sweep budget check removed",
        CONTRACT,
        "        if spent >= budget {\n"
        "            tracing::debug!(\n"
        "                spent,\n"
        "                budget,",
        "        if false {\n"
        "            tracing::debug!(\n"
        "                spent,\n"
        "                budget,",
        ["the_ttl_sweep_is_bounded_and_leaves_the_rest_for_the_next_pass"],
        "RED",
    ),
    (
        "TTL sweep reserves a maximal victim's cost BEFORE spending it",
        CONTRACT,
        "        if spent >= budget {\n"
        "            tracing::debug!(",
        "        if spent.saturating_add(delegate_park::MAX_PENDING_PER_DELEGATE) >= budget {\n"
        "            tracing::debug!(",
        ["the_sweep_budget_admits_one_maximal_victim_past_the_limit"],
        "RED",
    ),
    (
        "TTL sweep charges one per victim instead of its real run count",
        CONTRACT,
        "spent = spent.saturating_add(runs.max(1));",
        "spent = spent.saturating_add(1);",
        ["the_sweep_budget_admits_one_maximal_victim_past_the_limit"],
        "RED",
    ),
    (
        "unexpected executor response reported to the client as an empty success",
        CONTRACT,
        "                return DelegateRunOutcome::Failed(ExecutorError::other(anyhow::anyhow!(\n"
        '                    "unexpected response variant for a delegate request on {delegate_key}"\n'
        "                )));",
        "                return DelegateRunOutcome::Completed(Vec::new());",
        ["an_unexpected_executor_response_reaches_the_client_as_an_error"],
        "RED",
    ),
    (
        "M3 site 1: notification park delivers to a client responder that does not exist",
        CONTRACT,
        "            // No client behind a notification-driven run; residual messages fan\n"
        "            // out to registered apps instead.\n"
        "            delivery: delegate_park::Delivery::Apps,",
        "            delivery: delegate_park::Delivery::Client,",
        ["a_parked_notification_run_delivers_its_residual_output_to_apps"],
        "RED",
    ),
    (
        "M3 site 2: the same defect in run_queued_notification's own ParkingCtx",
        CONTRACT,
        "        delegate_key,\n"
        "        prompter,\n"
        "        park.map(|park| ParkingCtx {\n"
        "            park,\n"
        "            delivery: delegate_park::Delivery::Apps,",
        "        delegate_key,\n"
        "        prompter,\n"
        "        park.map(|park| ParkingCtx {\n"
        "            park,\n"
        "            delivery: delegate_park::Delivery::Client,",
        ["a_queued_notification_that_parks_also_delivers_to_apps"],
        "RED",
    ),
    (
        "owed_upserts drops the delegate's context",
        PARK,
        "            // ECHOED BACK on a synthesized failure. See `OwedUpsert::context`.\n"
        "            context: u.context.clone(),",
        "            context: Default::default(),",
        ["owed_upserts_carry_the_delegate_s_context"],
        "RED",
    ),
    (
        "owed_upserts drops the context, watched by the test that CANNOT see it",
        PARK,
        "            // ECHOED BACK on a synthesized failure. See `OwedUpsert::context`.\n"
        "            context: u.context.clone(),",
        "            context: Default::default(),",
        ["a_partially_resolved_upsert_pair_keeps_the_unresolved_one_s_context"],
        "GREEN",
    ),
    (
        "off-loop fetch allowance check removed",
        CONTRACT,
        "    if bytes > allowance {\n"
        "        let id = states",
        "    if false {\n        let id = states",
        ["an_oversized_related_fetch_is_refused_and_counted"],
        "RED",
    ),
    (
        "a tenth module-scope *_BYTES budget, summed nowhere",
        PARK,
        "const ELEMENT_OVERHEAD_BYTES: ByteCount = ByteCount::new(256);",
        "const ELEMENT_OVERHEAD_BYTES: ByteCount = ByteCount::new(256);\n"
        "#[allow(dead_code)]\n"
        "const MUTATION_PROBE_BYTES: usize = 1;",
        ["declared_cache_ceiling_discovers_every_budget"],
        "RED",
    ),
    (
        "a NOT_SUMMED entry naming a budget that does not exist",
        EXECUTOR,
        "        const NOT_SUMMED: &[(&str, &str)] = &[",
        "        const NOT_SUMMED: &[(&str, &str)] = &[\n"
        '            ("MUTATION_PROBE_STALE_BYTES", "names nothing that exists"),',
        ["declared_cache_ceiling_discovers_every_budget"],
        "RED",
    ),
    (
        "chokepoint reached from an unenumerated spawn form inside an allowed function",
        CONTRACT,
        "    delegate_app_registry::sweep_expired();",
        "    delegate_app_registry::sweep_expired();\n"
        "    if false {\n"
        "        tokio::task::spawn_local(async move {\n"
        '            let _ = "handle_delegate_with_contract_requests(";\n'
        "        });\n"
        "    }",
        ["every_delegate_run_is_reached_from_the_serial_loop"],
        "RED",
    ),
    (
        "chokepoint called from a new `pub async fn` declared after an allowed one",
        CONTRACT,
        "fn route_notification_outbound(delegate_key: &DelegateKey, outbound: Vec<OutboundDelegateMsg>) {",
        "#[allow(dead_code)]\n"
        "pub async fn mutation_probe_caller() {\n"
        '    let _ = "handle_delegate_with_contract_requests(";\n'
        "}\n\n"
        "fn route_notification_outbound(delegate_key: &DelegateKey, outbound: Vec<OutboundDelegateMsg>) {",
        ["every_delegate_run_is_reached_from_the_serial_loop"],
        "RED",
    ),
    (
        "the loop's buffered-resume `continue` guard deleted",
        CONTRACT,
        "        if !delegate_resumes.is_empty() {\n            continue;\n        }",
        "        if false {\n            continue;\n        }",
        ["the_loop_never_idles_while_holding_delegate_resumes"],
        "RED",
    ),
    (
        "the notification path's registry sweep made conditional, position unchanged",
        CONTRACT,
        "    delegate_app_registry::sweep_expired();",
        "    if false {\n        delegate_app_registry::sweep_expired();\n    }",
        ["the_notification_path_actually_runs_the_registry_sweep"],
        "RED",
    ),
    (
        "...and the SOURCE PIN for it, under the same mutation: the documented limit",
        CONTRACT,
        "    delegate_app_registry::sweep_expired();",
        "    if false {\n        delegate_app_registry::sweep_expired();\n    }",
        ["ttl_sweep_precedes_every_exit_from_the_notification_path"],
        "GREEN",
    ),
]

CAMPAIGNS = {"bytes": BYTES_CASES, "guards": GUARDS_CASES}


def head_sha() -> str:
    return subprocess.run(
        ["git", "-C", str(WORKTREE), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in CAMPAIGNS:
        raise SystemExit(f"usage: {sys.argv[0]} {{{'|'.join(CAMPAIGNS)}}}")
    name = sys.argv[1]
    tree = Tree(FILES)
    try:
        campaign(tree, CAMPAIGNS[name], f"{head_sha()} [{name}]")
    finally:
        tree.restore()


if __name__ == "__main__":
    main()
