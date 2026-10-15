//! `oar-server`: the unique OAR server daemon.

use std::process::ExitCode;

use oar_common::config::Config;
use oar_server::ServerError;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("oar-server: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), ServerError> {
    let config = Config::load(None)?;
    config.init_tracing();

    let nodes = config.node_list()?;
    tracing::info!(
        address = %config.server_address(),
        nodes = nodes.len(),
        scheduler = "fifo-placeholder",
        "starting oar-server"
    );

    let server = oar_server::start(config).await?;
    tracing::info!(endpoint = %server.endpoint(), "oar-server ready");

    tokio::signal::ctrl_c().await.ok();
    tracing::info!("shutting down");
    server.shutdown().await;
    Ok(())
}
