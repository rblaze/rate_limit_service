use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

use rate_limit_service::config::ServiceConfig;
use rate_limit_service::envoy::service::ratelimit::v3::rate_limit_service_server::RateLimitServiceServer;
use rate_limit_service::service::RlsProxyService;

#[derive(Debug, Parser)]
#[command(
    name = "rate-limit-service",
    about = "Envoy RateLimitService (CheckRateLimit & ConsumeRateLimit) proxy backed by Redis"
)]
struct Cli {
    /// Path to YAML configuration file.
    #[arg(short, long, default_value = "config/rls.yaml")]
    config: PathBuf,

    /// Optional override for the gRPC listen address (e.g. 0.0.0.0:18081).
    #[arg(short, long)]
    listen: Option<SocketAddr>,

    /// Default log level when RUST_LOG is not set.
    #[arg(long, default_value = "info")]
    log_level: String,
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sigterm) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sigterm.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    info!("received shutdown signal, stopping gRPC server");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cli.log_level));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let mut config = ServiceConfig::from_file(&cli.config)
        .with_context(|| format!("failed to load configuration from {}", cli.config.display()))?;
    if let Some(listen_override) = cli.listen {
        config.listen = listen_override;
    }

    let rls_service = RlsProxyService::from_config(&config)?;

    info!(
        listen = %config.listen,
        domain = %config.domain,
        redis = %config.redis.connection_url(),
        models = config.models.len(),
        "starting Envoy RateLimitService proxy"
    );

    Server::builder()
        .add_service(RateLimitServiceServer::new(rls_service))
        .serve_with_shutdown(config.listen, shutdown_signal())
        .await
        .context("gRPC server exited with error")?;

    Ok(())
}
