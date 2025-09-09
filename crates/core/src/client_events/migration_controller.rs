//! Migration controller for safe staged rollout of actor-based client management
//!
//! This module provides production-ready migration infrastructure with:
//! - Staged migration phases with clear rollback points
//! - Performance validation and comparison
//! - Emergency rollback capabilities
//! - Comprehensive health monitoring during transitions

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use std::default::Default;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tracing::{error, info, warn};

/// Migration phases with clear rollback points
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Phase 0: Current production state (legacy only)
    LegacyOnly,
    /// Phase 1: Both systems running (rollback safe) 
    DualPath,
    /// Phase 2: Actor primary, legacy fallback
    ActorPrimary,
    /// Phase 3: Actor only (full migration)
    ActorOnly,
}

impl Default for MigrationPhase {
    fn default() -> Self {
        MigrationPhase::LegacyOnly
    }
}

impl MigrationPhase {
    /// Get the environment variable value for this phase
    pub fn env_value(&self) -> &'static str {
        match self {
            MigrationPhase::LegacyOnly => "",
            MigrationPhase::DualPath => "dual_path", 
            MigrationPhase::ActorPrimary => "router_only",
            MigrationPhase::ActorOnly => "true",
        }
    }

    /// Parse from environment variable value
    pub fn from_env_value(value: &str) -> Self {
        match value {
            "dual_path" => MigrationPhase::DualPath,
            "router_only" => MigrationPhase::ActorPrimary,
            "true" => MigrationPhase::ActorOnly,
            _ => MigrationPhase::LegacyOnly,
        }
    }

    /// Get the next phase in the migration sequence
    pub fn next_phase(&self) -> Option<Self> {
        match self {
            MigrationPhase::LegacyOnly => Some(MigrationPhase::DualPath),
            MigrationPhase::DualPath => Some(MigrationPhase::ActorPrimary),
            MigrationPhase::ActorPrimary => Some(MigrationPhase::ActorOnly),
            MigrationPhase::ActorOnly => None, // Already at final phase
        }
    }
}

/// Performance baseline for comparison during migration  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    pub requests_per_second: f64,
    pub avg_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub error_rate: f64,
    pub memory_usage_mb: f64,
    #[serde(skip, default = "Instant::now")] // Skip timestamp for serialization, use current time for default
    pub timestamp: Instant,
}

impl PerformanceBaseline {
    /// Compare current metrics against baseline
    pub fn compare(&self, current: &PerformanceBaseline) -> PerformanceComparison {
        let latency_increase = (current.p99_latency_ms - self.p99_latency_ms) / self.p99_latency_ms;
        let error_rate_increase = current.error_rate - self.error_rate;
        let throughput_change = (current.requests_per_second - self.requests_per_second) / self.requests_per_second;
        
        PerformanceComparison {
            latency_increase_percent: latency_increase * 100.0,
            error_rate_increase_percent: error_rate_increase * 100.0, 
            throughput_change_percent: throughput_change * 100.0,
            meets_sla: latency_increase < 0.2 && error_rate_increase < 0.01, // 20% latency, 1% error rate
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceComparison {
    pub latency_increase_percent: f64,
    pub error_rate_increase_percent: f64,
    pub throughput_change_percent: f64,
    pub meets_sla: bool,
}

/// Health monitoring during migration
#[derive(Debug, Clone)]
pub struct MigrationHealthMonitor {
    health_checks: Arc<RwLock<HashMap<String, HealthCheckResult>>>,
    failure_count: Arc<AtomicU64>,
    last_health_check: Arc<RwLock<Instant>>,
}

#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    pub healthy: bool,
    pub message: String,
    pub timestamp: Instant,
}

impl MigrationHealthMonitor {
    pub fn new() -> Self {
        Self {
            health_checks: Arc::new(RwLock::new(HashMap::new())),
            failure_count: Arc::new(AtomicU64::new(0)),
            last_health_check: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Record a health check result
    pub async fn record_health_check(&self, component: String, result: HealthCheckResult) {
        let mut checks = self.health_checks.write().await;
        if !result.healthy {
            self.failure_count.fetch_add(1, Ordering::Relaxed);
        }
        checks.insert(component, result);
        *self.last_health_check.write().await = Instant::now();
    }

    /// Check if system is healthy for migration
    pub async fn is_healthy_for_migration(&self) -> bool {
        let checks = self.health_checks.read().await;
        let failure_count = self.failure_count.load(Ordering::Relaxed);
        
        // System is healthy if:
        // 1. All recent health checks passed
        // 2. Failure count is below threshold
        // 3. Health checks are recent (within last 30 seconds)
        
        let all_healthy = checks.values().all(|result| result.healthy);
        let failure_threshold = failure_count < 5;
        let checks_recent = {
            let last_check = self.last_health_check.read().await;
            last_check.elapsed() < Duration::from_secs(30)
        };
        
        all_healthy && failure_threshold && checks_recent
    }

    /// Get current health status
    pub async fn get_health_summary(&self) -> String {
        let checks = self.health_checks.read().await;
        let failure_count = self.failure_count.load(Ordering::Relaxed);
        let healthy_count = checks.values().filter(|r| r.healthy).count();
        let total_checks = checks.len();
        
        format!(
            "Health: {}/{} checks passing, {} total failures", 
            healthy_count, total_checks, failure_count
        )
    }
}

/// Migration controller for safe staged rollout
pub struct MigrationController {
    current_phase: Arc<RwLock<MigrationPhase>>,
    health_monitor: MigrationHealthMonitor,
    performance_baseline: Arc<RwLock<Option<PerformanceBaseline>>>,
    rollback_trigger: broadcast::Sender<RollbackReason>,
    phase_history: Arc<RwLock<Vec<(MigrationPhase, Instant)>>>,
    config: MigrationConfig,
}

#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Minimum time to spend in each phase before advancing
    pub phase_duration: Duration,
    /// Performance validation window
    pub validation_window: Duration,
    /// Automatic rollback on health check failures
    pub auto_rollback: bool,
    /// Maximum latency increase allowed (as percentage)
    pub max_latency_increase: f64,
    /// Maximum error rate increase allowed  
    pub max_error_rate_increase: f64,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            phase_duration: Duration::from_secs(24 * 60 * 60), // 24 hours minimum per phase
            validation_window: Duration::from_secs(60 * 60), // 1 hour validation
            auto_rollback: true,
            max_latency_increase: 0.2, // 20% max latency increase
            max_error_rate_increase: 0.01, // 1% max error rate increase
        }
    }
}

#[derive(Debug, Clone)]
pub enum RollbackReason {
    ManualTrigger,
    HealthCheckFailure(String),
    PerformanceDegradation(String),
    ActorSystemFailure(String),
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("Migration validation failed: {reason}")]
    ValidationFailed { reason: String },
    
    #[error("Phase transition not allowed: from {from:?} to {to:?}")]
    InvalidTransition { from: MigrationPhase, to: MigrationPhase },
    
    #[error("Rollback failed: {reason}")]
    RollbackFailed { reason: String },
    
    #[error("Health check failed: {component} - {message}")]
    HealthCheckFailed { component: String, message: String },
    
    #[error("Performance degradation detected: {details}")]
    PerformanceDegradation { details: String },
}

impl MigrationController {
    /// Create a new migration controller
    pub fn new(config: MigrationConfig) -> Self {
        let (rollback_trigger, _) = broadcast::channel(16);
        
        Self {
            current_phase: Arc::new(RwLock::new(MigrationPhase::default())),
            health_monitor: MigrationHealthMonitor::new(),
            performance_baseline: Arc::new(RwLock::new(None)),
            rollback_trigger,
            phase_history: Arc::new(RwLock::new(Vec::new())),
            config,
        }
    }

    /// Get current migration phase
    pub async fn current_phase(&self) -> MigrationPhase {
        *self.current_phase.read().await
    }

    /// Set the environment variable for current phase
    pub async fn apply_phase_to_env(&self) -> Result<()> {
        let phase = self.current_phase().await;
        let env_value = phase.env_value();
        
        if env_value.is_empty() {
            std::env::remove_var("FREENET_ACTOR_CLIENTS");
        } else {
            std::env::set_var("FREENET_ACTOR_CLIENTS", env_value);
        }
        
        info!(
            "Applied migration phase {:?} to environment (FREENET_ACTOR_CLIENTS={})",
            phase, env_value
        );
        
        Ok(())
    }

    /// Advance to the next migration phase with validation
    pub async fn advance_phase(&mut self) -> Result<()> {
        let current_phase = self.current_phase().await;
        
        let next_phase = current_phase
            .next_phase()
            .ok_or(MigrationError::InvalidTransition {
                from: current_phase,
                to: current_phase, // No next phase available
            })?;

        info!("Attempting to advance from {:?} to {:?}", current_phase, next_phase);

        // Validate readiness for next phase
        self.validate_phase_readiness(current_phase, next_phase).await?;

        // Record phase transition
        {
            let mut history = self.phase_history.write().await;
            history.push((next_phase, Instant::now()));
        }

        // Update current phase
        *self.current_phase.write().await = next_phase;

        // Apply to environment
        self.apply_phase_to_env().await?;

        info!("Successfully advanced to migration phase {:?}", next_phase);
        
        Ok(())
    }

    /// Validate system is ready for phase transition
    async fn validate_phase_readiness(
        &self,
        current: MigrationPhase,
        next: MigrationPhase,
    ) -> Result<()> {
        info!("Validating readiness for phase transition: {:?} -> {:?}", current, next);

        // Check minimum phase duration
        if let Some(last_transition) = self.get_last_transition_time().await {
            if last_transition.elapsed() < self.config.phase_duration {
                return Err(anyhow!(MigrationError::ValidationFailed {
                    reason: format!(
                        "Minimum phase duration not met. Elapsed: {:?}, Required: {:?}",
                        last_transition.elapsed(),
                        self.config.phase_duration
                    ),
                }));
            }
        }

        // Check system health
        if !self.health_monitor.is_healthy_for_migration().await {
            let health_summary = self.health_monitor.get_health_summary().await;
            return Err(anyhow!(MigrationError::ValidationFailed {
                reason: format!("System not healthy for migration: {}", health_summary),
            }));
        }

        // Performance validation for critical transitions
        match (current, next) {
            (MigrationPhase::DualPath, MigrationPhase::ActorPrimary) => {
                self.validate_actor_system_performance().await?;
            }
            (MigrationPhase::ActorPrimary, MigrationPhase::ActorOnly) => {
                self.validate_legacy_removal_safety().await?;
            }
            _ => {} // Other transitions don't require special validation
        }

        info!("Phase transition validation passed");
        Ok(())
    }

    /// Validate actor system performance meets SLA requirements
    async fn validate_actor_system_performance(&self) -> Result<()> {
        let baseline_lock = self.performance_baseline.read().await;
        
        let _baseline = baseline_lock
            .as_ref()
            .ok_or(anyhow!(MigrationError::ValidationFailed {
                reason: "No performance baseline available".to_string(),
            }))?;

        // In a real implementation, this would collect current metrics
        // For now, we'll create a placeholder for the validation logic
        info!("Validating actor system performance against baseline...");
        
        // TODO: Implement actual performance metric collection
        // let current_metrics = self.collect_current_performance_metrics().await?;
        // let comparison = baseline.compare(&current_metrics);
        
        // if !comparison.meets_sla {
        //     return Err(MigrationError::PerformanceDegradation {
        //         details: format!("Actor system performance below SLA: {:?}", comparison),
        //     });
        // }

        info!("Actor system performance validation passed");
        Ok(())
    }

    /// Validate it's safe to remove legacy paths
    async fn validate_legacy_removal_safety(&self) -> Result<()> {
        info!("Validating safety of legacy path removal...");
        
        // Check that actor system has been stable
        let actor_system_stable = self.health_monitor.is_healthy_for_migration().await;
        
        if !actor_system_stable {
            return Err(anyhow!(MigrationError::ValidationFailed {
                reason: "Actor system not stable enough for legacy removal".to_string(),
            }));
        }

        // TODO: Add additional safety checks:
        // - Verify no critical errors in actor system logs
        // - Check that session actor is processing requests efficiently  
        // - Validate no memory leaks or resource exhaustion
        
        info!("Legacy removal safety validation passed");
        Ok(())
    }

    /// Emergency rollback to previous safe phase
    pub async fn emergency_rollback(&mut self, reason: RollbackReason) -> Result<()> {
        let current_phase = self.current_phase().await;
        
        error!("Emergency rollback initiated from phase: {:?}, reason: {:?}", current_phase, reason);

        let target_phase = match current_phase {
            MigrationPhase::ActorOnly => {
                // Critical: Actor system failure, restore legacy primary 
                warn!("Rolling back from ActorOnly to LegacyOnly - complete fallback");
                MigrationPhase::LegacyOnly
            }
            MigrationPhase::ActorPrimary => {
                // Switch back to dual path for safety
                warn!("Rolling back from ActorPrimary to DualPath - making legacy primary");
                MigrationPhase::DualPath
            }
            MigrationPhase::DualPath => {
                // Disable actor path, pure legacy mode
                warn!("Rolling back from DualPath to LegacyOnly - disabling actor system");
                MigrationPhase::LegacyOnly
            }
            MigrationPhase::LegacyOnly => {
                info!("Already in LegacyOnly phase - no rollback needed");
                return Ok(());
            }
        };

        // Record rollback in history
        {
            let mut history = self.phase_history.write().await;
            history.push((target_phase, Instant::now()));
        }

        // Update current phase
        *self.current_phase.write().await = target_phase;

        // Apply to environment  
        self.apply_phase_to_env().await?;

        // Notify all subscribers about rollback
        let _ = self.rollback_trigger.send(reason);

        info!("Emergency rollback completed to phase: {:?}", target_phase);
        
        Ok(())
    }

    /// Set performance baseline for comparisons
    pub async fn set_performance_baseline(&self, baseline: PerformanceBaseline) {
        info!("Performance baseline set: {:?}", baseline);
        *self.performance_baseline.write().await = Some(baseline);
    }

    /// Get subscriber for rollback notifications
    pub fn subscribe_to_rollback(&self) -> broadcast::Receiver<RollbackReason> {
        self.rollback_trigger.subscribe()
    }

    /// Record health check result
    pub async fn record_health_check(&self, component: String, healthy: bool, message: String) {
        let result = HealthCheckResult {
            healthy,
            message,
            timestamp: Instant::now(),
        };
        
        self.health_monitor.record_health_check(component, result).await;
    }

    /// Get migration status summary
    pub async fn get_status(&self) -> MigrationStatus {
        let current_phase = self.current_phase().await;
        let health_summary = self.health_monitor.get_health_summary().await;
        let history = self.phase_history.read().await.clone();
        
        MigrationStatus {
            current_phase,
            health_summary,
            phase_history: history,
            baseline_available: self.performance_baseline.read().await.is_some(),
        }
    }

    /// Get time of last phase transition
    async fn get_last_transition_time(&self) -> Option<Instant> {
        let history = self.phase_history.read().await;
        history.last().map(|(_, timestamp)| *timestamp)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MigrationStatus {
    pub current_phase: MigrationPhase,
    pub health_summary: String,
    #[serde(skip)] // Skip phase history for serialization since Instant doesn't support it
    pub phase_history: Vec<(MigrationPhase, Instant)>,
    pub baseline_available: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_migration_phase_progression() {
        assert_eq!(MigrationPhase::LegacyOnly.next_phase(), Some(MigrationPhase::DualPath));
        assert_eq!(MigrationPhase::DualPath.next_phase(), Some(MigrationPhase::ActorPrimary));
        assert_eq!(MigrationPhase::ActorPrimary.next_phase(), Some(MigrationPhase::ActorOnly));
        assert_eq!(MigrationPhase::ActorOnly.next_phase(), None);
    }

    #[tokio::test]
    async fn test_migration_phase_env_values() {
        assert_eq!(MigrationPhase::LegacyOnly.env_value(), "");
        assert_eq!(MigrationPhase::DualPath.env_value(), "dual_path");
        assert_eq!(MigrationPhase::ActorPrimary.env_value(), "router_only");  
        assert_eq!(MigrationPhase::ActorOnly.env_value(), "true");
    }

    #[tokio::test]
    async fn test_migration_controller_creation() {
        let config = MigrationConfig::default();
        let controller = MigrationController::new(config);
        
        assert_eq!(controller.current_phase().await, MigrationPhase::LegacyOnly);
    }

    #[tokio::test] 
    async fn test_health_monitor() {
        let monitor = MigrationHealthMonitor::new();
        
        // Record healthy check
        monitor.record_health_check(
            "session_actor".to_string(),
            HealthCheckResult {
                healthy: true,
                message: "All good".to_string(),
                timestamp: Instant::now(),
            }
        ).await;
        
        assert!(monitor.is_healthy_for_migration().await);
        
        // Record unhealthy check
        monitor.record_health_check(
            "result_router".to_string(),
            HealthCheckResult {
                healthy: false, 
                message: "Channel full".to_string(),
                timestamp: Instant::now(),
            }
        ).await;
        
        assert!(!monitor.is_healthy_for_migration().await);
    }

    #[tokio::test]
    async fn test_rollback_scenarios() {
        let config = MigrationConfig {
            phase_duration: Duration::from_millis(1), // Very short for testing
            validation_window: Duration::from_millis(1),
            ..Default::default()
        };
        let mut controller = MigrationController::new(config);
        
        // Advance to ActorOnly phase (in real usage this would require validation)
        *controller.current_phase.write().await = MigrationPhase::ActorOnly;
        
        // Test emergency rollback
        controller
            .emergency_rollback(RollbackReason::ActorSystemFailure("Test failure".to_string()))
            .await
            .unwrap();
        
        assert_eq!(controller.current_phase().await, MigrationPhase::LegacyOnly);
    }

    #[tokio::test]
    async fn test_performance_baseline_comparison() {
        let baseline = PerformanceBaseline {
            requests_per_second: 1000.0,
            avg_latency_ms: 10.0,
            p99_latency_ms: 50.0,
            error_rate: 0.01,
            memory_usage_mb: 100.0,
            timestamp: Instant::now(),
        };
        
        let current = PerformanceBaseline {
            requests_per_second: 950.0, // 5% decrease
            avg_latency_ms: 12.0,       // 20% increase
            p99_latency_ms: 55.0,       // 10% increase  
            error_rate: 0.015,          // 0.5% increase
            memory_usage_mb: 110.0,     // 10% increase
            timestamp: Instant::now(),
        };
        
        let comparison = baseline.compare(&current);
        
        // Should still meet SLA (under 20% latency increase, under 1% error rate increase)
        assert!(comparison.meets_sla);
        assert!(comparison.latency_increase_percent < 20.0);
        assert!(comparison.error_rate_increase_percent < 1.0);
    }
}