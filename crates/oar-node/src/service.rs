//! The `ComputeNodeService` exposed by every compute node.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use oar_common::config::Config;
use oar_common::types::JobId;
use oar_proto::compute_node_service_server::ComputeNodeService;
use oar_proto::{Ack, Empty, PingResponse, StartJobRequest};
use tonic::{Request, Response, Status};

use crate::callback::NodeCallback;
use crate::executor::{Executor, JobExecution};

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error(transparent)]
    Config(#[from] oar_common::config::ConfigError),
    #[error(transparent)]
    Callback(#[from] crate::callback::CallbackError),
    #[error(transparent)]
    Grpc(#[from] oar_common::grpc::GrpcError),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Everything a node knows about itself.
pub struct NodeState {
    node_id: String,
    address: String,
    running_jobs: AtomicU32,
    executor: Arc<dyn Executor>,
    callback: Arc<dyn NodeCallback>,
    config: Arc<Config>,
}

impl std::fmt::Debug for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeState")
            .field("node_id", &self.node_id)
            .field("address", &self.address)
            .field("running_jobs", &self.running_jobs.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl NodeState {
    pub fn new(
        node_id: impl Into<String>,
        address: impl Into<String>,
        executor: Arc<dyn Executor>,
        callback: Arc<dyn NodeCallback>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            address: address.into(),
            running_jobs: AtomicU32::new(0),
            executor,
            callback,
            config,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn running_jobs(&self) -> u32 {
        self.running_jobs.load(Ordering::SeqCst)
    }

    pub fn executor(&self) -> &Arc<dyn Executor> {
        &self.executor
    }

    pub fn callback(&self) -> &Arc<dyn NodeCallback> {
        &self.callback
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Records a completion: decrements the running counter and notifies the
    /// server. Errors are logged, never propagated to the gRPC caller.
    pub async fn complete_job(&self, job_id: JobId, exit_code: i32) {
        self.running_jobs.fetch_sub(1, Ordering::SeqCst);
        if let Err(error) = self
            .callback
            .job_finished(job_id, &self.node_id, exit_code)
            .await
        {
            tracing::warn!(job_id, %error, "could not report the job completion to the server");
        }
    }
}

/// Builds a [`JobExecution`] out of a wire request, or explains why it is invalid.
pub fn execution_from_request(request: &StartJobRequest) -> Result<JobExecution, String> {
    if request.command.trim().is_empty() {
        return Err("empty command".into());
    }
    if request.nodes.is_empty() {
        return Err("a job must target at least one node".into());
    }
    Ok(JobExecution {
        job_id: request.job_id,
        user_uid: request.user_uid,
        command: request.command.clone(),
        nodes: request.nodes.iter().map(|node| node.id.clone()).collect(),
        walltime_secs: request.walltime_secs,
    })
}

/// gRPC service exposed by an `oar-node` daemon.
#[derive(Debug, Clone)]
pub struct ComputeNodeGrpc {
    state: Arc<NodeState>,
}

impl ComputeNodeGrpc {
    pub fn new(state: Arc<NodeState>) -> Self {
        Self { state }
    }

    pub fn state(&self) -> &Arc<NodeState> {
        &self.state
    }
}

#[tonic::async_trait]
impl ComputeNodeService for ComputeNodeGrpc {
    async fn start_job(&self, request: Request<StartJobRequest>) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        let execution = match execution_from_request(&request) {
            Ok(execution) => execution,
            Err(message) => return Ok(Response::new(Ack { ok: false, message })),
        };

        let job_id = execution.job_id;
        self.state.running_jobs.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            job_id = execution.job_id,
            nodes = execution.nodes.len(),
            "accepted StartJob"
        );

        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let exit_code = state.executor.run(&execution).await;
            state.complete_job(execution.job_id, exit_code).await;
        });

        Ok(Response::new(Ack {
            ok: true,
            message: format!("job {job_id} accepted by {}", self.state.node_id),
        }))
    }

    async fn ping(&self, _request: Request<Empty>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            node_id: self.state.node_id.clone(),
            running_jobs: self.state.running_jobs(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use oar_proto::NodeRef;
    use tokio::sync::mpsc;

    use crate::callback::CallbackError;
    use crate::executor::FakeExecutor;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Completion {
        job_id: JobId,
        node_id: String,
        exit_code: i32,
    }

    #[derive(Default)]
    struct FakeCallback {
        completions: Mutex<Vec<Completion>>,
        tx: Mutex<Option<mpsc::UnboundedSender<Completion>>>,
    }

    impl FakeCallback {
        fn with_notifier(tx: mpsc::UnboundedSender<Completion>) -> Self {
            Self {
                tx: Mutex::new(Some(tx)),
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl NodeCallback for FakeCallback {
        async fn register(&self, _node_id: &str, _address: &str) -> Result<(), CallbackError> {
            Ok(())
        }

        async fn heartbeat(&self, _node_id: &str, _running: u32) -> Result<(), CallbackError> {
            Ok(())
        }

        async fn job_finished(
            &self,
            job_id: JobId,
            node_id: &str,
            exit_code: i32,
        ) -> Result<(), CallbackError> {
            let completion = Completion {
                job_id,
                node_id: node_id.to_string(),
                exit_code,
            };
            self.completions.lock().unwrap().push(completion.clone());
            if let Some(tx) = self.tx.lock().unwrap().as_ref() {
                let _ = tx.send(completion);
            }
            Ok(())
        }
    }

    fn request(command: &str) -> StartJobRequest {
        StartJobRequest {
            job_id: 5,
            user_uid: 1000,
            command: command.into(),
            nodes: vec![
                NodeRef {
                    id: "n1".into(),
                    address: "127.0.0.1:7001".into(),
                },
                NodeRef {
                    id: "n2".into(),
                    address: "127.0.0.1:7002".into(),
                },
            ],
            walltime_secs: 60,
        }
    }

    fn state(notifier: Option<mpsc::UnboundedSender<Completion>>) -> Arc<NodeState> {
        let callback: Arc<dyn NodeCallback> = match notifier {
            Some(tx) => Arc::new(FakeCallback::with_notifier(tx)),
            None => Arc::new(FakeCallback::default()),
        };
        Arc::new(NodeState::new(
            "n1",
            "127.0.0.1:7001",
            Arc::new(FakeExecutor::new(Duration::ZERO)),
            callback,
            Arc::new(Config::default()),
        ))
    }

    #[test]
    fn request_validation() {
        assert!(execution_from_request(&request("echo hi")).is_ok());
        assert!(execution_from_request(&request("   ")).is_err());
        let mut empty = request("x");
        empty.nodes.clear();
        assert!(execution_from_request(&empty).is_err());
        let execution = execution_from_request(&request("echo hi")).unwrap();
        assert_eq!(execution.head_node().map(String::as_str), Some("n1"));
    }

    #[tokio::test]
    async fn start_job_runs_the_executor_and_reports_the_exit_code() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let state = state(Some(tx));
        let service = ComputeNodeGrpc::new(Arc::clone(&state));

        let ack = service
            .start_job(Request::new(request("exit=3")))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.ok);

        let completion = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("the node must report the completion")
            .unwrap();
        assert_eq!(completion.job_id, 5);
        assert_eq!(completion.node_id, "n1");
        assert_eq!(completion.exit_code, 3);
        assert_eq!(state.running_jobs(), 0);
    }

    #[tokio::test]
    async fn invalid_start_job_is_refused_without_running_anything() {
        let state = state(None);
        let service = ComputeNodeGrpc::new(Arc::clone(&state));

        let ack = service
            .start_job(Request::new(request("  ")))
            .await
            .unwrap()
            .into_inner();
        assert!(!ack.ok);
        assert_eq!(state.running_jobs(), 0);
    }

    #[tokio::test]
    async fn ping_reports_the_node_and_its_load() {
        let state = state(None);
        let service = ComputeNodeGrpc::new(state);
        let pong = service
            .ping(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(pong.node_id, "n1");
        assert_eq!(pong.running_jobs, 0);
    }
}
