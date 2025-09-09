//! Admin endpoints for migration and operational control
//!
//! This module provides REST API endpoints for operators to:
//! - Control migration phases
//! - Trigger emergency rollbacks  
//! - Monitor system health
//! - Apply emergency overrides

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::migration_controller::{
    MigrationController, MigrationPhase, MigrationStatus, PerformanceBaseline, RollbackReason,
};

/// Shared admin state
pub struct AdminState {
    pub migration_controller: Arc<RwLock<MigrationController>>,
}

/// Admin API responses
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message),
        }
    }
}

/// Migration control requests
#[derive(Debug, Deserialize)]
pub struct AdvancePhaseRequest {
    pub force: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct RollbackRequest {
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct SetBaselineRequest {
    pub requests_per_second: f64,
    pub avg_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub error_rate: f64,
    pub memory_usage_mb: f64,
}

#[derive(Debug, Deserialize)]
pub struct HealthCheckRequest {
    pub component: String,
    pub healthy: bool,
    pub message: String,
}

/// Build the admin API router
pub fn build_admin_router(state: AdminState) -> Router {
    Router::new()
        .route("/migration/status", get(get_migration_status))
        .route("/migration/advance", post(advance_migration_phase))
        .route("/migration/rollback", post(trigger_rollback))
        .route("/migration/baseline", post(set_performance_baseline))
        .route("/health/:component", post(record_health_check))
        .route("/health", get(get_health_status))
        .with_state(state)
}

/// Get current migration status
async fn get_migration_status(
    State(state): State<AdminState>,
) -> Result<Json<ApiResponse<MigrationStatus>>, StatusCode> {
    let controller = state.migration_controller.read().await;
    let status = controller.get_status().await;
    
    Ok(Json(ApiResponse::success(status)))
}

/// Advance to next migration phase
async fn advance_migration_phase(
    State(state): State<AdminState>,
    Json(request): Json<AdvancePhaseRequest>,
) -> Result<Json<ApiResponse<MigrationPhase>>, StatusCode> {
    let mut controller = state.migration_controller.write().await;
    
    match controller.advance_phase().await {
        Ok(()) => {
            let new_phase = controller.current_phase().await;
            tracing::info!(
                "Migration phase advanced to {:?} via admin API (force: {:?})",
                new_phase,
                request.force
            );
            Ok(Json(ApiResponse::success(new_phase)))
        }
        Err(e) => {
            tracing::error!("Failed to advance migration phase: {}", e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Trigger emergency rollback
async fn trigger_rollback(
    State(state): State<AdminState>,
    Json(request): Json<RollbackRequest>,
) -> Result<Json<ApiResponse<MigrationPhase>>, StatusCode> {
    let mut controller = state.migration_controller.write().await;
    
    let rollback_reason = RollbackReason::ManualTrigger;
    
    match controller.emergency_rollback(rollback_reason).await {
        Ok(()) => {
            let current_phase = controller.current_phase().await;
            tracing::warn!(
                "Emergency rollback completed to {:?} via admin API. Reason: {}",
                current_phase,
                request.reason
            );
            Ok(Json(ApiResponse::success(current_phase)))
        }
        Err(e) => {
            tracing::error!("Emergency rollback failed: {}", e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Set performance baseline for migration validation
async fn set_performance_baseline(
    State(state): State<AdminState>,
    Json(request): Json<SetBaselineRequest>,
) -> Result<Json<ApiResponse<()>>, StatusCode> {
    let controller = state.migration_controller.read().await;
    
    let baseline = PerformanceBaseline {
        requests_per_second: request.requests_per_second,
        avg_latency_ms: request.avg_latency_ms,
        p99_latency_ms: request.p99_latency_ms,
        error_rate: request.error_rate,
        memory_usage_mb: request.memory_usage_mb,
        timestamp: std::time::Instant::now(),
    };
    
    controller.set_performance_baseline(baseline).await;
    
    tracing::info!(
        "Performance baseline set via admin API: {:.0} req/s, {:.1}ms p99",
        request.requests_per_second,
        request.p99_latency_ms
    );
    
    Ok(Json(ApiResponse::success(())))
}

/// Record health check result
async fn record_health_check(
    State(state): State<AdminState>,
    Path(component): Path<String>,
    Json(request): Json<HealthCheckRequest>,
) -> Result<Json<ApiResponse<()>>, StatusCode> {
    let controller = state.migration_controller.read().await;
    
    controller
        .record_health_check(component.clone(), request.healthy, request.message.clone())
        .await;
    
    tracing::debug!(
        "Health check recorded via admin API: {} = {} ({})",
        component,
        request.healthy,
        request.message
    );
    
    Ok(Json(ApiResponse::success(())))
}

/// Get overall health status
async fn get_health_status(
    State(state): State<AdminState>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let controller = state.migration_controller.read().await;
    let health_summary = controller.get_status().await.health_summary;
    
    Ok(Json(ApiResponse::success(health_summary)))
}

/// Admin CLI utilities for operational tasks
pub mod cli {
    use super::*;
    use clap::{Parser, Subcommand};

    #[derive(Parser)]
    #[command(name = "freenet-admin")]
    #[command(about = "Freenet node administration tools")]
    pub struct AdminCli {
        #[command(subcommand)]
        pub command: AdminCommand,
        
        #[arg(long, default_value = "http://localhost:8080")]
        pub endpoint: String,
    }

    #[derive(Subcommand)]
    pub enum AdminCommand {
        /// Migration management
        Migration {
            #[command(subcommand)]
            action: MigrationAction,
        },
        /// Health monitoring
        Health {
            #[command(subcommand)]
            action: HealthAction,
        },
    }

    #[derive(Subcommand)]
    pub enum MigrationAction {
        /// Get current migration status
        Status,
        /// Advance to next phase
        Advance {
            #[arg(long)]
            force: bool,
        },
        /// Trigger emergency rollback
        Rollback {
            #[arg(long)]
            reason: String,
        },
        /// Set performance baseline
        SetBaseline {
            #[arg(long)]
            requests_per_second: f64,
            #[arg(long)]  
            p99_latency_ms: f64,
            #[arg(long)]
            error_rate: f64,
            #[arg(long)]
            memory_mb: f64,
        },
    }

    #[derive(Subcommand)]
    pub enum HealthAction {
        /// Get health status
        Status,
        /// Record health check
        Record {
            component: String,
            #[arg(long)]
            healthy: bool,
            #[arg(long)]
            message: String,
        },
    }

    /// Execute admin CLI commands
    pub async fn execute_command(cli: AdminCli) -> Result<(), Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();
        let base_url = cli.endpoint;

        match cli.command {
            AdminCommand::Migration { action } => {
                execute_migration_command(&client, &base_url, action).await?;
            }
            AdminCommand::Health { action } => {
                execute_health_command(&client, &base_url, action).await?;
            }
        }

        Ok(())
    }

    async fn execute_migration_command(
        client: &reqwest::Client,
        base_url: &str,
        action: MigrationAction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match action {
            MigrationAction::Status => {
                let response = client
                    .get(&format!("{}/admin/migration/status", base_url))
                    .send()
                    .await?;
                
                let status: ApiResponse<MigrationStatus> = response.json().await?;
                println!("Migration Status: {:#?}", status.data);
            }
            
            MigrationAction::Advance { force } => {
                let request = AdvancePhaseRequest { force: Some(force) };
                let response = client
                    .post(&format!("{}/admin/migration/advance", base_url))
                    .json(&request)
                    .send()
                    .await?;
                
                let result: ApiResponse<MigrationPhase> = response.json().await?;
                if result.success {
                    println!("Migration advanced to: {:?}", result.data);
                } else {
                    eprintln!("Migration advance failed: {}", result.error.unwrap_or_default());
                }
            }
            
            MigrationAction::Rollback { reason } => {
                let request = RollbackRequest { reason };
                let response = client
                    .post(&format!("{}/admin/migration/rollback", base_url))
                    .json(&request)
                    .send()
                    .await?;
                
                let result: ApiResponse<MigrationPhase> = response.json().await?;
                if result.success {
                    println!("Rollback completed to: {:?}", result.data);
                } else {
                    eprintln!("Rollback failed: {}", result.error.unwrap_or_default());
                }
            }
            
            MigrationAction::SetBaseline { requests_per_second, p99_latency_ms, error_rate, memory_mb } => {
                let request = SetBaselineRequest {
                    requests_per_second,
                    avg_latency_ms: p99_latency_ms / 2.0, // Rough estimate
                    p99_latency_ms,
                    error_rate,
                    memory_usage_mb: memory_mb,
                };
                
                let response = client
                    .post(&format!("{}/admin/migration/baseline", base_url))
                    .json(&request)
                    .send()
                    .await?;
                
                if response.status().is_success() {
                    println!("Performance baseline set successfully");
                } else {
                    eprintln!("Failed to set baseline: {}", response.status());
                }
            }
        }

        Ok(())
    }

    async fn execute_health_command(
        client: &reqwest::Client,
        base_url: &str,
        action: HealthAction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match action {
            HealthAction::Status => {
                let response = client
                    .get(&format!("{}/admin/health", base_url))
                    .send()
                    .await?;
                
                let health: ApiResponse<String> = response.json().await?;
                println!("Health Status: {}", health.data.unwrap_or_default());
            }
            
            HealthAction::Record { component, healthy, message } => {
                let request = HealthCheckRequest { component: component.clone(), healthy, message };
                let response = client
                    .post(&format!("{}/admin/health/{}", base_url, component))
                    .json(&request)
                    .send()
                    .await?;
                
                if response.status().is_success() {
                    println!("Health check recorded for {}: {}", component, healthy);
                } else {
                    eprintln!("Failed to record health check: {}", response.status());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum_test::TestServer;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    async fn create_test_server() -> TestServer {
        let migration_controller = MigrationController::new(Default::default());
        let state = AdminState {
            migration_controller: Arc::new(RwLock::new(migration_controller)),
        };
        
        let app = build_admin_router(state);
        TestServer::new(app).unwrap()
    }

    #[tokio::test]
    async fn test_migration_status_endpoint() {
        let server = create_test_server().await;
        
        let response = server.get("/migration/status").await;
        response.assert_status_ok();
        
        let json: ApiResponse<MigrationStatus> = response.json();
        assert!(json.success);
        assert_eq!(json.data.unwrap().current_phase, MigrationPhase::LegacyOnly);
    }

    #[tokio::test]
    async fn test_health_check_recording() {
        let server = create_test_server().await;
        
        let health_request = HealthCheckRequest {
            component: "session_actor".to_string(),
            healthy: true,
            message: "All systems operational".to_string(),
        };
        
        let response = server
            .post("/health/session_actor")
            .json(&health_request)
            .await;
            
        response.assert_status_ok();
        
        let json: ApiResponse<()> = response.json();
        assert!(json.success);
    }

    #[tokio::test]
    async fn test_performance_baseline_setting() {
        let server = create_test_server().await;
        
        let baseline_request = SetBaselineRequest {
            requests_per_second: 1000.0,
            avg_latency_ms: 10.0,
            p99_latency_ms: 50.0,
            error_rate: 0.01,
            memory_usage_mb: 100.0,
        };
        
        let response = server
            .post("/migration/baseline")
            .json(&baseline_request)
            .await;
            
        response.assert_status_ok();
        
        let json: ApiResponse<()> = response.json();
        assert!(json.success);
    }
}