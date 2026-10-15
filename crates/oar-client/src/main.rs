//! `oar-client`: the client daemon (JSON over unix domain socket).

use std::process::ExitCode;
use std::sync::Arc;

use oar_client::{ClientDaemon, ClientError, GrpcServerClient, start};
use oar_common::config::Config;
use oar_common::db::MemoryDatabase;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("oar-client: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), ClientError> {
    let config = Config::load(None)?;
    config.init_tracing();

    // Placeholder for the shared PostgreSQL database of OAR3.
    let database = Arc::new(MemoryDatabase::new());
    let server = Arc::new(GrpcServerClient::new(config.server_address())?);
    let daemon = Arc::new(ClientDaemon::new(
        database,
        server,
        Arc::new(config.clone()),
    ));

    let socket = config.socket_path();
    let client = start(daemon, &socket).await?;
    tracing::info!(socket = %client.socket().display(), "oar-client ready");

    tokio::signal::ctrl_c().await.ok();
    tracing::info!("shutting down");
    client.shutdown().await;
    Ok(())
}
