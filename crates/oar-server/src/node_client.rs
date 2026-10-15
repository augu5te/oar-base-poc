//! Server -> compute node calls.
//!
//! [`NodeLauncher`] hides the generated `ComputeNodeService` client so the
//! engine can be unit-tested with a fake, while [`GrpcNodeLauncher`] is the
//! production implementation.

use std::time::Duration;

use async_trait::async_trait;
use oar_common::grpc;
use oar_common::types::{Job, NodeInfo};
use oar_proto::compute_node_service_client::ComputeNodeServiceClient;
use oar_proto::{Empty, NodeRef, StartJobRequest};

#[derive(Debug, thiserror::Error)]
pub enum LauncherError {
    #[error("node {node} is unreachable: {reason}")]
    Unreachable { node: String, reason: String },
    #[error("node {node} refused the job: {reason}")]
    Rejected { node: String, reason: String },
}

impl LauncherError {
    pub fn unreachable(node: &NodeInfo, reason: impl std::fmt::Display) -> Self {
        Self::Unreachable {
            node: node.id.clone(),
            reason: reason.to_string(),
        }
    }

    pub fn rejected(node: &NodeInfo, reason: impl std::fmt::Display) -> Self {
        Self::Rejected {
            node: node.id.clone(),
            reason: reason.to_string(),
        }
    }
}

/// Result of a successful liveness probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePing {
    pub node_id: String,
    pub running_jobs: u32,
}

/// Sends `StartJob` / `Ping` to compute nodes.
#[async_trait]
pub trait NodeLauncher: Send + Sync + 'static {
    /// Asks the head node of `job` to start it. Every node of the job is passed
    /// in the request; the receiver is the head node.
    async fn start_job(&self, node: &NodeInfo, job: &Job) -> Result<(), LauncherError>;

    /// Probes a node; `Ok` means the node answered (and is therefore alive).
    async fn ping(&self, node: &NodeInfo) -> Result<NodePing, LauncherError>;
}

/// gRPC implementation, connecting to `NodeInfo::address`.
#[derive(Debug, Clone)]
pub struct GrpcNodeLauncher {
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl Default for GrpcNodeLauncher {
    fn default() -> Self {
        Self::new()
    }
}

impl GrpcNodeLauncher {
    pub fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_timeouts(connect_timeout: Duration, request_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            request_timeout,
        }
    }

    async fn client(
        &self,
        node: &NodeInfo,
    ) -> Result<ComputeNodeServiceClient<tonic::transport::Channel>, LauncherError> {
        let endpoint = grpc::endpoint(&node.address)
            .map_err(|error| LauncherError::unreachable(node, error))?;
        let channel = endpoint
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .connect()
            .await
            .map_err(|error| LauncherError::unreachable(node, error))?;
        Ok(ComputeNodeServiceClient::new(channel))
    }
}

/// Builds the wire request for `StartJob`.
pub fn start_job_request(job: &Job) -> StartJobRequest {
    StartJobRequest {
        job_id: job.id,
        user_uid: job.request.user_uid,
        command: job.request.command.clone(),
        nodes: job
            .assigned_nodes
            .iter()
            .map(|id| NodeRef {
                id: id.clone(),
                address: String::new(),
            })
            .collect(),
        walltime_secs: job.request.walltime_secs,
    }
}

#[async_trait]
impl NodeLauncher for GrpcNodeLauncher {
    async fn start_job(&self, node: &NodeInfo, job: &Job) -> Result<(), LauncherError> {
        let mut client = self.client(node).await?;
        let response = client
            .start_job(start_job_request(job))
            .await
            .map_err(|status| LauncherError::rejected(node, status))?
            .into_inner();
        if response.ok {
            Ok(())
        } else {
            Err(LauncherError::rejected(node, response.message))
        }
    }

    async fn ping(&self, node: &NodeInfo) -> Result<NodePing, LauncherError> {
        let mut client = self.client(node).await?;
        let response = client
            .ping(Empty {})
            .await
            .map_err(|status| LauncherError::unreachable(node, status))?
            .into_inner();
        Ok(NodePing {
            node_id: if response.node_id.is_empty() {
                node.id.clone()
            } else {
                response.node_id
            },
            running_jobs: response.running_jobs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oar_common::types::JobRequest;

    #[test]
    fn start_job_request_lists_every_node_head_first() {
        let mut job = Job::new(42, JobRequest::new(1000, "echo hi", 2, 3600));
        job.assigned_nodes = vec!["n1".into(), "n2".into()];
        let request = start_job_request(&job);
        assert_eq!(request.job_id, 42);
        assert_eq!(request.user_uid, 1000);
        assert_eq!(request.command, "echo hi");
        assert_eq!(request.walltime_secs, 3600);
        assert_eq!(request.nodes.len(), 2);
        assert_eq!(request.nodes[0].id, "n1");
    }

    #[tokio::test]
    async fn unreachable_node_is_reported() {
        let launcher =
            GrpcNodeLauncher::with_timeouts(Duration::from_millis(200), Duration::from_millis(200));
        // Port 1 is reserved and nothing listens there.
        let node = NodeInfo::new("dead", "127.0.0.1:1");
        let job = Job::new(1, JobRequest::new(0, "x", 1, 60));
        assert!(matches!(
            launcher.start_job(&node, &job).await,
            Err(LauncherError::Unreachable { .. })
        ));
        assert!(matches!(
            launcher.ping(&node).await,
            Err(LauncherError::Unreachable { .. })
        ));
    }
}
