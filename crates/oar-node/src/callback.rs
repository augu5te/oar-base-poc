//! Node -> server notifications: registration, heartbeat and job completion.
//!
//! [`NodeCallback`] hides the generated `NodeCallbackService` client so the node
//! service can be tested with a fake.

use async_trait::async_trait;
use oar_common::grpc;
use oar_common::types::JobId;
use oar_proto::node_callback_service_client::NodeCallbackServiceClient;
use oar_proto::{HeartbeatRequest, JobFinishedRequest, RegisterNodeRequest};
use tonic::transport::Channel;

#[derive(Debug, thiserror::Error)]
pub enum CallbackError {
    #[error("invalid server address `{address}`: {reason}")]
    Address { address: String, reason: String },
    #[error("cannot reach the server: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("the server refused the notification: {0}")]
    Rejected(String),
}

/// Notifies the server about this node.
#[async_trait]
pub trait NodeCallback: Send + Sync + 'static {
    async fn register(&self, node_id: &str, address: &str) -> Result<(), CallbackError>;
    async fn heartbeat(&self, node_id: &str, running_jobs: u32) -> Result<(), CallbackError>;
    async fn job_finished(
        &self,
        job_id: JobId,
        node_id: &str,
        exit_code: i32,
    ) -> Result<(), CallbackError>;
}

/// gRPC implementation talking to the unique server's `NodeCallbackService`.
#[derive(Debug, Clone)]
pub struct GrpcNodeCallback {
    server_address: String,
    channel: Channel,
}

impl GrpcNodeCallback {
    /// Builds a lazy client for `server_address` (`host:port` or a full URI).
    pub fn new(server_address: impl Into<String>) -> Result<Self, CallbackError> {
        let server_address = server_address.into();
        let channel =
            grpc::connect_lazy(&server_address).map_err(|error| CallbackError::Address {
                address: server_address.clone(),
                reason: error.to_string(),
            })?;
        Ok(Self {
            server_address,
            channel,
        })
    }

    pub fn server_address(&self) -> &str {
        &self.server_address
    }

    fn client(&self) -> NodeCallbackServiceClient<Channel> {
        NodeCallbackServiceClient::new(self.channel.clone())
    }
}

#[async_trait]
impl NodeCallback for GrpcNodeCallback {
    async fn register(&self, node_id: &str, address: &str) -> Result<(), CallbackError> {
        let ack = self
            .client()
            .register_node(RegisterNodeRequest {
                node_id: node_id.into(),
                address: address.into(),
            })
            .await
            .map_err(|status| CallbackError::Rejected(status.to_string()))?
            .into_inner();
        if ack.ok {
            Ok(())
        } else {
            Err(CallbackError::Rejected(ack.message))
        }
    }

    async fn heartbeat(&self, node_id: &str, running_jobs: u32) -> Result<(), CallbackError> {
        let ack = self
            .client()
            .heartbeat(HeartbeatRequest {
                node_id: node_id.into(),
                running_jobs,
            })
            .await
            .map_err(|status| CallbackError::Rejected(status.to_string()))?
            .into_inner();
        if ack.ok {
            Ok(())
        } else {
            Err(CallbackError::Rejected(ack.message))
        }
    }

    async fn job_finished(
        &self,
        job_id: JobId,
        node_id: &str,
        exit_code: i32,
    ) -> Result<(), CallbackError> {
        let ack = self
            .client()
            .job_finished(JobFinishedRequest {
                job_id,
                node_id: node_id.into(),
                exit_code,
            })
            .await
            .map_err(|status| CallbackError::Rejected(status.to_string()))?
            .into_inner();
        // A refused completion (unknown / already finished job) is not fatal.
        if ack.ok {
            Ok(())
        } else {
            tracing::warn!(job_id, message = %ack.message, "server refused the job completion");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lazy_client_can_be_built_without_a_server() {
        let callback = GrpcNodeCallback::new("127.0.0.1:6666").unwrap();
        assert_eq!(callback.server_address(), "127.0.0.1:6666");
        // `new` never connects; also accept full URIs.
        assert!(GrpcNodeCallback::new("http://127.0.0.1:6666").is_ok());
    }

    #[tokio::test]
    async fn notifications_fail_cleanly_when_the_server_is_absent() {
        let callback = GrpcNodeCallback::new("127.0.0.1:1").unwrap();
        assert!(callback.register("n1", "127.0.0.1:7001").await.is_err());
        assert!(callback.heartbeat("n1", 0).await.is_err());
        assert!(callback.job_finished(1, "n1", 0).await.is_err());
    }
}
