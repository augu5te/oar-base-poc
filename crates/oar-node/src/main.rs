//! `oar-node`: the compute node daemon.

use std::process::ExitCode;

use oar_common::config::Config;
use oar_node::NodeError;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("oar-node: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), NodeError> {
    let config = Config::load(None)?;
    config.init_tracing();

    let node = oar_node::start(config).await?;
    tracing::info!(
        node_id = %node.node_id,
        address = %node.address,
        endpoint = %node.endpoint(),
        "oar-node ready"
    );

    oar_node::wait_for_shutdown().await;
    tracing::info!("shutting down");
    node.shutdown().await;
    Ok(())
}
