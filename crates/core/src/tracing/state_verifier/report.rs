//! Verification report and summary display.

use super::types::ContractStateHistory;
use super::types::StateAnomaly;

/// Full verification report.
#[derive(Debug, Clone)]
pub struct VerificationReport {
    /// Per-contract state history.
    pub contract_histories: Vec<ContractStateHistory>,
    /// All detected anomalies.
    pub anomalies: Vec<StateAnomaly>,
    /// Total events analyzed.
    pub total_events: usize,
    /// Total state-mutating events.
    pub state_events: usize,
    /// Number of contracts analyzed.
    pub contracts_analyzed: usize,
}

impl VerificationReport {
    /// Returns true if no anomalies were detected.
    pub fn is_clean(&self) -> bool {
        self.anomalies.is_empty()
    }

    /// Returns only divergence anomalies.
    pub fn divergences(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::FinalDivergence { .. }))
            .collect()
    }

    /// Returns only missing broadcast anomalies.
    pub fn missing_broadcasts(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::MissingBroadcast { .. }))
            .collect()
    }

    /// Returns only unapplied broadcast anomalies.
    pub fn unapplied_broadcasts(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::BroadcastNotApplied { .. }))
            .collect()
    }

    pub fn suspected_partitions(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::SuspectedPartition { .. }))
            .collect()
    }

    pub fn stale_peers(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::StalePeer { .. }))
            .collect()
    }

    pub fn state_oscillations(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::StateOscillation { .. }))
            .collect()
    }

    pub fn zombie_transactions(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::ZombieTransaction { .. }))
            .collect()
    }

    pub fn broadcast_storms(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::BroadcastStorm { .. }))
            .collect()
    }

    pub fn delta_sync_cascades(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::DeltaSyncFailureCascade { .. }))
            .collect()
    }

    /// Produce a human-readable summary.
    pub fn display(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "=== State Verification Report ===\n\
             Events analyzed: {} ({} state-mutating)\n\
             Contracts: {}\n\
             Anomalies: {}\n",
            self.total_events,
            self.state_events,
            self.contracts_analyzed,
            self.anomalies.len(),
        ));

        if self.anomalies.is_empty() {
            out.push_str("  No anomalies detected. All contracts converged.\n");
        } else {
            let missing = self.missing_broadcasts().len();
            let unapplied = self.unapplied_broadcasts().len();
            let divergences = self.divergences().len();
            let unexpected = self
                .anomalies
                .iter()
                .filter(|a| matches!(a, StateAnomaly::UnexpectedStateChange { .. }))
                .count();

            out.push_str(&format!(
                "  Missing broadcasts: {}\n\
                 Unapplied broadcasts: {}\n\
                 Unexpected state changes: {}\n\
                 Final divergences: {}\n",
                missing, unapplied, unexpected, divergences
            ));
        }

        for history in &self.contract_histories {
            out.push_str(&format!(
                "\n--- Contract: {} ---\n\
                 Transitions: {}, Broadcasts emitted: {}\n\
                 Final states: {}\n",
                history.contract_key,
                history.transitions.len(),
                history.emitted_broadcasts.len(),
                history
                    .final_peer_states
                    .iter()
                    .map(|(addr, hash)| format!("  {}={}", addr, hash))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ));
        }

        if !self.anomalies.is_empty() {
            out.push_str("\n--- Anomalies ---\n");
            for (i, anomaly) in self.anomalies.iter().enumerate() {
                out.push_str(&format!("  [{}] {}\n", i + 1, anomaly));
            }
        }

        out
    }
}
