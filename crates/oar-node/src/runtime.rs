//! Node wiring: compute-node gRPC endpoint + registration/heartbeat loop.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use oar_common::config::Config;
use oar_common::grpc;
use oar_proto::compute_node_service_server::ComputeNodeServiceServer;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

use crate::callback::GrpcNodeCallback;
use crate::executor::FakeExecutor;
use crate::service::{ComputeNodeGrpc, NodeError, NodeState};

/// A running compute node daemon.
pub struct RunningNode {
    pub node_id: String,
    pub address: String,
    endpoint: String,
    state: Arc<NodeState>,
    shutdown: Arc<Notify>,
    announce: JoinHandle<()>,
    grpc: JoinHandle<Result<(), NodeError>>,
}

impl std::fmt::Debug for RunningNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningNode")
            .field("node_id", &self.node_id)
            .field("address", &self.address)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl RunningNode {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn state(&self) -> &Arc<NodeState> {
        &self.state
    }

    /// Stops the gRPC endpoint and the announce loop.
    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
        self.announce.abort();
        match self.grpc.await {
            Ok(Ok(())) => tracing::info!("node gRPC server stopped"),
            Ok(Err(error)) => tracing::warn!(%error, "node gRPC server stopped with an error"),
            Err(error) if !error.is_cancelled() => tracing::warn!(%error, "node gRPC task failed"),
            Err(_) => {}
        }
    }
}

async fn announce_loop(state: Arc<NodeState>, shutdown: Arc<Notify>, interval: Duration) {
    let mut registered = false;
    loop {
        if !registered {
            match state
                .callback()
                .register(state.node_id(), state.address())
                .await
            {
                Ok(()) => {
                    registered = true;
                    tracing::info!(node_id = state.node_id(), "registered with the server");
                }
                Err(error) => {
                    tracing::warn!(%error, "registration failed, retrying");
                }
            }
        } else {
            match state
                .callback()
                .heartbeat(state.node_id(), state.running_jobs())
                .await
            {
                Ok(()) => tracing::trace!("heartbeat sent"),
                Err(error) => {
                    registered = false;
                    tracing::warn!(%error, "heartbeat failed, re-registering");
                }
            }
        }

        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            () = shutdown.notified() => break,
        }
    }
    tracing::debug!("announce loop stopped");
}

fn build_state(config: &Arc<Config>, address: String) -> Result<Arc<NodeState>, NodeError> {
    let callback = Arc::new(GrpcNodeCallback::new(config.server_address())?);
    let executor = Arc::new(FakeExecutor::new(Duration::from_secs(
        config.simulated_job_duration_secs.max(1),
    )));
    Ok(Arc::new(NodeState::new(
        config.resolved_node_id(),
        address,
        executor,
        callback,
        Arc::clone(config),
    )))
}

async fn spawn_announce(state: &Arc<NodeState>, shutdown: &Arc<Notify>) -> JoinHandle<()> {
    let interval = Duration::from_secs(state.config().node_heartbeat_secs.max(1));
    tokio::spawn(announce_loop(
        Arc::clone(state),
        Arc::clone(shutdown),
        interval,
    ))
}

/// Starts the node on `config.node_address` (port `0` asks for a free port).
pub async fn start(config: Config) -> Result<RunningNode, NodeError> {
    let config = Arc::new(config);
    let listener = TcpListener::bind(&config.node_address).await?;
    let local_addr = listener.local_addr()?;
    let address = local_addr.to_string();
    let state = build_state(&config, address.clone())?;
    let shutdown = Arc::new(Notify::new());
    let announce = spawn_announce(&state, &shutdown).await;

    let incoming = TcpIncoming::from(listener);
    let grpc_state = Arc::clone(&state);
    let signal = Arc::clone(&shutdown);
    let grpc = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ComputeNodeServiceServer::new(ComputeNodeGrpc::new(
                grpc_state,
            )))
            .serve_with_incoming_shutdown(incoming, async move { signal.notified().await })
            .await
            .map_err(NodeError::from)
    });

    Ok(RunningNode {
        node_id: state.node_id().to_string(),
        address,
        endpoint: format!("http://{local_addr}"),
        state,
        shutdown,
        announce,
        grpc,
    })
}

/// Starts the node gRPC endpoint on a unix domain socket.
pub async fn start_unix(config: Config, socket: &Path) -> Result<RunningNode, NodeError> {
    let config = Arc::new(config);
    let address = grpc::unix_address(socket);
    let state = build_state(&config, address.clone())?;
    let shutdown = Arc::new(Notify::new());
    let announce = spawn_announce(&state, &shutdown).await;

    let listener = grpc::bind_unix(socket)?;
    let incoming = grpc::unix_incoming(listener);
    let grpc_state = Arc::clone(&state);
    let signal = Arc::clone(&shutdown);
    let grpc = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ComputeNodeServiceServer::new(ComputeNodeGrpc::new(
                grpc_state,
            )))
            .serve_with_incoming_shutdown(incoming, async move { signal.notified().await })
            .await
            .map_err(NodeError::from)
    });

    Ok(RunningNode {
        node_id: state.node_id().to_string(),
        address,
        endpoint: grpc::unix_address(socket),
        state,
        shutdown,
        announce,
        grpc,
    })
}

/// Convenience used by `main` to keep the node alive until a signal.
pub async fn wait_for_shutdown() {
    tokio::signal::ctrl_c().await.ok();
}
