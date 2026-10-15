//! The client daemon: JSON-over-UDS protocol, local job cache, server calls.
//!
//! One `oar-client` process runs per client machine. The CLI commands connect to
//! its unix domain socket and exchange one JSON document per line
//! ([`oar_common::ipc`]). For every submission the daemon:
//!
//! 1. takes the submitter's identity from the socket credentials (`SO_PEERCRED`);
//! 2. notifies the server, which allocates the definitive job id;
//! 3. stores the submission in the (placeholder) database.

use std::path::Path;
use std::sync::Arc;

use oar_common::config::Config;
use oar_common::db::Database;
use oar_common::ipc::{
    IpcError, IpcRequest, IpcResponse, JobSummary, read_line_bounded, write_message,
};
use oar_common::types::{Job, JobFilter, JobId, JobRequest, JobState};
use tokio::io::BufReader;
use tokio::net::{UnixListener, UnixStream};

use crate::backend::{ClientError, ServerClient};

/// Handles one IPC request on behalf of one user.
pub struct ClientDaemon {
    db: Arc<dyn Database>,
    server: Arc<dyn ServerClient>,
    config: Arc<Config>,
}

impl std::fmt::Debug for ClientDaemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientDaemon").finish_non_exhaustive()
    }
}

impl ClientDaemon {
    pub fn new(db: Arc<dyn Database>, server: Arc<dyn ServerClient>, config: Arc<Config>) -> Self {
        Self { db, server, config }
    }

    pub fn database(&self) -> &Arc<dyn Database> {
        &self.db
    }

    pub fn server(&self) -> &Arc<dyn ServerClient> {
        &self.server
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Builds the [`JobRequest`] for a submission, applying the configured
    /// defaults to missing values, and validates it.
    pub fn build_request(
        &self,
        user_uid: u32,
        command: String,
        nodes: u32,
        walltime_secs: u32,
    ) -> Result<JobRequest, oar_common::types::ValidationError> {
        let request = JobRequest {
            user_uid,
            command,
            nodes: if nodes == 0 { 1 } else { nodes },
            walltime_secs: if walltime_secs == 0 {
                self.config.default_walltime_secs
            } else {
                walltime_secs
            },
        };
        request.validate()?;
        Ok(request)
    }

    /// Processes one request. Never fails: errors become [`IpcResponse::Error`].
    pub async fn handle(&self, request: IpcRequest, user_uid: u32) -> IpcResponse {
        match self.try_handle(request, user_uid).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%error, "request failed");
                IpcResponse::Error {
                    message: error.to_string(),
                }
            }
        }
    }

    async fn try_handle(
        &self,
        request: IpcRequest,
        user_uid: u32,
    ) -> Result<IpcResponse, ClientError> {
        match request {
            IpcRequest::Submit {
                command,
                nodes,
                walltime_secs,
            } => {
                let request = self.build_request(user_uid, command, nodes, walltime_secs)?;
                let job_id = self.server.notify_new_job(&request).await?;
                // Cache the submission locally (placeholder for the shared DB).
                let mut job = Job::new(job_id, request);
                job.state = JobState::Waiting;
                self.db.insert_job(job).await?;
                tracing::info!(job_id, user_uid, "job submitted");
                Ok(IpcResponse::Submitted {
                    job_id,
                    warning: None,
                })
            }
            IpcRequest::Status { job_id } => {
                match self.server.get_job(job_id).await {
                    Ok(Some(summary)) => {
                        self.cache(&summary).await;
                        return Ok(IpcResponse::Status { job: summary });
                    }
                    Ok(None) => {}
                    Err(error) => {
                        // The server is unreachable: answer from the local cache.
                        tracing::warn!(%error, "status from the local cache");
                    }
                }
                match self.local(job_id).await? {
                    Some(summary) => Ok(IpcResponse::Status { job: summary }),
                    None => Ok(IpcResponse::Error {
                        message: format!("unknown job {job_id}"),
                    }),
                }
            }
            IpcRequest::List { state } => {
                match self.server.list_jobs(state).await {
                    Ok(jobs) => {
                        for job in &jobs {
                            self.cache(job).await;
                        }
                        Ok(IpcResponse::Jobs { jobs })
                    }
                    Err(error) => {
                        // The server is unreachable: answer from the local cache.
                        tracing::warn!(%error, "listing from the local cache");
                        let filter = JobFilter {
                            state,
                            user_uid: None,
                        };
                        let jobs = self.db.list_jobs(&filter).await?;
                        Ok(IpcResponse::Jobs {
                            jobs: jobs
                                .iter()
                                .map(oar_common::convert::job_to_summary)
                                .collect(),
                        })
                    }
                }
            }
        }
    }

    /// Refreshes the local copy of a job with the server's view.
    async fn cache(&self, summary: &JobSummary) {
        let Ok(Some(mut job)) = self.db.get_job(summary.job_id).await else {
            return;
        };
        job.state = summary.state;
        job.assigned_nodes = summary.nodes.clone();
        job.exit_code = summary.exit_code;
        if let Err(error) = self.db.update_job(&job).await {
            tracing::debug!(%error, "could not refresh the local job cache");
        }
    }

    async fn local(&self, job_id: JobId) -> Result<Option<JobSummary>, ClientError> {
        Ok(self
            .db
            .get_job(job_id)
            .await?
            .map(|job| oar_common::convert::job_to_summary(&job)))
    }
}

/// Serves one connection: one request per line, one response per line, until
/// the peer closes the socket.
pub async fn handle_connection(
    stream: UnixStream,
    daemon: Arc<ClientDaemon>,
) -> Result<(), IpcError> {
    let user_uid = stream
        .peer_cred()
        .map(|credentials| credentials.uid())
        .unwrap_or(0);
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    while let Some(line) = read_line_bounded(&mut reader).await? {
        let response = match serde_json::from_str::<IpcRequest>(&line) {
            Ok(request) => daemon.handle(request, user_uid).await,
            Err(error) => IpcResponse::Error {
                message: format!("malformed request: {error}"),
            },
        };
        write_message(&mut writer, &response).await?;
    }
    Ok(())
}

/// Accepts connections on `socket` forever.
pub async fn serve(daemon: Arc<ClientDaemon>, socket: &Path) -> Result<(), ClientError> {
    serve_with_shutdown(daemon, socket, std::future::pending::<()>()).await
}

/// Accepts connections until `shutdown` completes.
pub async fn serve_with_shutdown(
    daemon: Arc<ClientDaemon>,
    socket: &Path,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), ClientError> {
    let listener = bind_socket(socket)?;
    serve_listener(daemon, listener, shutdown).await
}

/// Accepts connections on an already-bound listener until `shutdown` completes.
pub async fn serve_listener(
    daemon: Arc<ClientDaemon>,
    listener: UnixListener,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), ClientError> {
    tracing::info!("client daemon listening");
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let daemon = Arc::clone(&daemon);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, daemon).await {
                        tracing::debug!(%error, "ipc connection ended with an error");
                    }
                });
            }
            () = &mut shutdown => break,
        }
    }
    Ok(())
}

/// Binds the daemon socket, creating the parent directory and replacing a stale
/// socket file.
pub fn bind_socket(socket: &Path) -> Result<UnixListener, ClientError> {
    oar_common::grpc::bind_unix(socket).map_err(|error| match error {
        oar_common::grpc::GrpcError::Io { source, .. } => ClientError::Io(source),
        other => ClientError::Unavailable(other.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use oar_common::db::MemoryDatabase;

    #[derive(Default)]
    struct FakeServer {
        next_id: Mutex<JobId>,
        submitted: Mutex<Vec<JobRequest>>,
        jobs: Mutex<Vec<JobSummary>>,
        reachable: Mutex<bool>,
    }

    impl FakeServer {
        fn new() -> Self {
            Self {
                next_id: Mutex::new(1),
                reachable: Mutex::new(true),
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl ServerClient for FakeServer {
        async fn notify_new_job(&self, request: &JobRequest) -> Result<JobId, ClientError> {
            if !*self.reachable.lock().unwrap() {
                return Err(ClientError::Unavailable("down".into()));
            }
            self.submitted.lock().unwrap().push(request.clone());
            let mut next = self.next_id.lock().unwrap();
            let job_id = *next;
            *next += 1;
            self.jobs.lock().unwrap().push(JobSummary {
                job_id,
                state: JobState::Waiting,
                nodes: Vec::new(),
                exit_code: None,
            });
            Ok(job_id)
        }

        async fn get_job(&self, job_id: JobId) -> Result<Option<JobSummary>, ClientError> {
            if !*self.reachable.lock().unwrap() {
                return Err(ClientError::Unavailable("down".into()));
            }
            Ok(self
                .jobs
                .lock()
                .unwrap()
                .iter()
                .find(|job| job.job_id == job_id)
                .cloned())
        }

        async fn list_jobs(&self, state: Option<JobState>) -> Result<Vec<JobSummary>, ClientError> {
            if !*self.reachable.lock().unwrap() {
                return Err(ClientError::Unavailable("down".into()));
            }
            Ok(self
                .jobs
                .lock()
                .unwrap()
                .iter()
                .filter(|job| state.is_none_or(|state| job.state == state))
                .cloned()
                .collect())
        }
    }

    fn daemon(server: Arc<FakeServer>) -> ClientDaemon {
        ClientDaemon::new(
            Arc::new(MemoryDatabase::new()),
            server,
            Arc::new(Config::default()),
        )
    }

    fn submit(command: &str, nodes: u32, walltime_secs: u32) -> IpcRequest {
        IpcRequest::Submit {
            command: command.into(),
            nodes,
            walltime_secs,
        }
    }

    #[tokio::test]
    async fn submit_notifies_the_server_and_caches_the_job() {
        let server = Arc::new(FakeServer::new());
        let daemon = daemon(server.clone());

        let response = daemon.handle(submit("echo hi", 2, 60), 1000).await;
        assert_eq!(
            response,
            IpcResponse::Submitted {
                job_id: 1,
                warning: None
            }
        );

        {
            let requests = server.submitted.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].user_uid, 1000,
                "the uid comes from the socket, not the message"
            );
            assert_eq!(requests[0].nodes, 2);
        }

        // Locally cached.
        let job = daemon.database().get_job(1).await.unwrap().unwrap();
        assert_eq!(job.state, JobState::Waiting);
    }

    #[tokio::test]
    async fn defaults_are_applied_and_invalid_requests_rejected() {
        let server = Arc::new(FakeServer::new());
        let daemon = daemon(server);

        let request = daemon.build_request(1000, "x".into(), 0, 0).unwrap();
        assert_eq!(request.nodes, 1);
        assert_eq!(
            request.walltime_secs,
            Config::default().default_walltime_secs
        );

        let response = daemon.handle(submit("   ", 1, 60), 1000).await;
        assert!(matches!(response, IpcResponse::Error { .. }));
    }

    #[tokio::test]
    async fn status_and_list_round_trip() {
        let server = Arc::new(FakeServer::new());
        let daemon = daemon(server.clone());
        daemon.handle(submit("echo one", 1, 60), 1000).await;
        daemon.handle(submit("echo two", 1, 60), 1000).await;

        let response = daemon.handle(IpcRequest::Status { job_id: 1 }, 1000).await;
        match response {
            IpcResponse::Status { job } => assert_eq!(job.job_id, 1),
            other => panic!("unexpected response: {other:?}"),
        }

        let response = daemon.handle(IpcRequest::List { state: None }, 1000).await;
        match response {
            IpcResponse::Jobs { jobs } => assert_eq!(jobs.len(), 2),
            other => panic!("unexpected response: {other:?}"),
        }

        let response = daemon.handle(IpcRequest::Status { job_id: 99 }, 1000).await;
        assert!(matches!(response, IpcResponse::Error { .. }));
    }

    #[tokio::test]
    async fn an_unreachable_server_falls_back_to_the_local_cache() {
        let server = Arc::new(FakeServer::new());
        let daemon = daemon(server.clone());
        daemon.handle(submit("echo hi", 1, 60), 1000).await;

        *server.reachable.lock().unwrap() = false;
        let response = daemon.handle(IpcRequest::List { state: None }, 1000).await;
        match response {
            IpcResponse::Jobs { jobs } => assert_eq!(jobs.len(), 1),
            other => panic!("unexpected response: {other:?}"),
        }

        let response = daemon.handle(IpcRequest::Status { job_id: 1 }, 1000).await;
        assert!(matches!(response, IpcResponse::Status { .. }));

        let response = daemon.handle(submit("echo hi", 1, 60), 1000).await;
        assert!(matches!(response, IpcResponse::Error { .. }));
    }
}
