//! Server wiring: engine + automaton + gRPC endpoints.
//!
//! [`start`] runs the whole server on a TCP endpoint; [`start_unix`] exposes the
//! same services on a unix domain socket. Both return a [`RunningServer`] handle
//! used by `main` and by the end-to-end tests.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use oar_common::config::Config;
use oar_common::db::MemoryDatabase;
use oar_common::grpc;
use oar_common::registry::NodeRegistry;
use oar_common::types::now_unix_secs;
use oar_proto::admin_service_server::AdminServiceServer;
use oar_proto::node_callback_service_server::NodeCallbackServiceServer;
use oar_proto::scheduler_service_server::SchedulerServiceServer;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

use crate::ServerError;
use crate::automaton::{Automaton, Event};
use crate::engine::{Clock, Engine, EngineBuilder, EngineSteps};
use crate::node_client::GrpcNodeLauncher;
use crate::scheduler::FifoScheduler;
use crate::services::{AdminGrpc, NodeCallbackGrpc, SchedulerGrpc};

/// A running server: the engine, its automaton and the gRPC endpoint.
pub struct RunningServer {
    pub engine: Arc<Engine>,
    endpoint: String,
    local_addr: Option<std::net::SocketAddr>,
    steps: Arc<EngineSteps>,
    automaton: JoinHandle<()>,
    shutdown: Option<oneshot::Sender<()>>,
    grpc: JoinHandle<Result<(), ServerError>>,
}

impl std::fmt::Debug for RunningServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningServer")
            .field("endpoint", &self.endpoint)
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl RunningServer {
    /// gRPC address (`http://host:port`) or `unix:` URI the server listens on.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The bound TCP address, when the server runs on TCP.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.local_addr
    }

    /// Stops the gRPC endpoint, the automaton and its workers.
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.steps.shutdown();
        self.automaton.abort();
        match self.grpc.await {
            Ok(Ok(())) => tracing::info!("gRPC server stopped"),
            Ok(Err(error)) => tracing::warn!(%error, "gRPC server stopped with an error"),
            Err(error) if !error.is_cancelled() => {
                tracing::warn!(%error, "gRPC server task failed")
            }
            Err(_) => {}
        }
    }
}

/// Builds the engine (placeholder database, static registry, FIFO scheduler) and
/// the receiving end of its event queue.
fn build_engine(
    config: &Arc<Config>,
) -> Result<(Arc<Engine>, mpsc::UnboundedReceiver<Event>), ServerError> {
    let nodes = config.node_list()?;
    let registry = Arc::new(NodeRegistry::with_nodes(nodes));
    let clock: Clock = Arc::new(now_unix_secs);
    Ok(EngineBuilder::new()
        .config(Arc::clone(config))
        .database(Arc::new(MemoryDatabase::new()))
        .registry(registry)
        .launcher(Arc::new(GrpcNodeLauncher::new()))
        .scheduler(Arc::new(FifoScheduler::new()))
        .clock(clock)
        .build())
}

fn services(
    engine: &Arc<Engine>,
) -> (
    SchedulerServiceServer<SchedulerGrpc>,
    NodeCallbackServiceServer<NodeCallbackGrpc>,
    AdminServiceServer<AdminGrpc>,
) {
    (
        SchedulerServiceServer::new(SchedulerGrpc::new(Arc::clone(engine))),
        NodeCallbackServiceServer::new(NodeCallbackGrpc::new(Arc::clone(engine))),
        AdminServiceServer::new(AdminGrpc::new(Arc::clone(engine))),
    )
}

async fn spawn_automaton(
    engine: &Arc<Engine>,
    events: mpsc::UnboundedReceiver<Event>,
) -> (Arc<EngineSteps>, JoinHandle<()>) {
    let steps = EngineSteps::start(Arc::clone(engine));
    let wait = Duration::from_secs(engine.config().automaton_wait_secs.max(1));
    let automaton = tokio::spawn(Automaton::new(steps.clone(), events, wait).run());
    (steps, automaton)
}

/// Starts the server on `config.server_hostname:config.server_port` (port `0`
/// asks the OS for a free port). The real address is available through
/// [`RunningServer::local_addr`].
pub async fn start(config: Config) -> Result<RunningServer, ServerError> {
    let config = Arc::new(config);
    let (engine, events) = build_engine(&config)?;
    let (steps, automaton) = spawn_automaton(&engine, events).await;

    let bind = format!("{}:{}", config.server_hostname, config.server_port);
    let listener = TcpListener::bind(&bind).await.map_err(ServerError::Io)?;
    let local_addr = listener.local_addr().map_err(ServerError::Io)?;
    let incoming = TcpIncoming::from(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (scheduler, node_callback, admin) = services(&engine);
    let router = tonic::transport::Server::builder()
        .add_service(scheduler)
        .add_service(node_callback)
        .add_service(admin);
    let grpc = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(ServerError::from)
    });

    Ok(RunningServer {
        engine,
        endpoint: format!("http://{local_addr}"),
        local_addr: Some(local_addr),
        steps,
        automaton,
        shutdown: Some(shutdown_tx),
        grpc,
    })
}

/// Starts the server exposing the gRPC services on a unix domain socket.
pub async fn start_unix(config: Config, socket: &Path) -> Result<RunningServer, ServerError> {
    let config = Arc::new(config);
    let (engine, events) = build_engine(&config)?;
    let (steps, automaton) = spawn_automaton(&engine, events).await;

    let listener = grpc::bind_unix(socket)?;
    let incoming = grpc::unix_incoming(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (scheduler, node_callback, admin) = services(&engine);
    let router = tonic::transport::Server::builder()
        .add_service(scheduler)
        .add_service(node_callback)
        .add_service(admin);
    let grpc = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(ServerError::from)
    });

    Ok(RunningServer {
        engine,
        endpoint: grpc::unix_address(socket),
        local_addr: None,
        steps,
        automaton,
        shutdown: Some(shutdown_tx),
        grpc,
    })
}
