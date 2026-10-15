//! Client daemon wiring: bind the UDS, serve until shutdown.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::backend::ClientError;
use crate::daemon::{ClientDaemon, bind_socket, serve_listener};

/// A running client daemon.
pub struct RunningClient {
    socket: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), ClientError>>,
}

impl std::fmt::Debug for RunningClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningClient")
            .field("socket", &self.socket)
            .finish_non_exhaustive()
    }
}

impl RunningClient {
    /// Path of the unix domain socket the daemon listens on.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Stops accepting connections.
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        match self.task.await {
            Ok(Ok(())) => tracing::info!("client daemon stopped"),
            Ok(Err(error)) => tracing::warn!(%error, "client daemon stopped with an error"),
            Err(error) if !error.is_cancelled() => {
                tracing::warn!(%error, "client daemon task failed")
            }
            Err(_) => {}
        }
    }
}

/// Binds `socket` and starts serving the IPC protocol on it.
pub async fn start(daemon: Arc<ClientDaemon>, socket: &Path) -> Result<RunningClient, ClientError> {
    let listener = bind_socket(socket)?;
    let (shutdown, receiver) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        serve_listener(daemon, listener, async move {
            let _ = receiver.await;
        })
        .await
    });
    tracing::info!(socket = %socket.display(), "oar-client ready");
    Ok(RunningClient {
        socket: socket.to_path_buf(),
        shutdown: Some(shutdown),
        task,
    })
}
