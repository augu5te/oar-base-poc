//! Client daemon -> server calls.
//!
//! [`ServerClient`] is the small surface the daemon needs; [`GrpcServerClient`]
//! is the gRPC implementation.

use async_trait::async_trait;
use oar_common::convert;
use oar_common::grpc;
use oar_common::ipc::JobSummary;
use oar_common::types::{JobId, JobRequest, JobState};
use oar_proto::admin_service_client::AdminServiceClient;
use oar_proto::scheduler_service_client::SchedulerServiceClient;
use oar_proto::{GetJobRequest, ListJobsRequest, NotifyNewJobRequest};
use tonic::transport::Channel;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("invalid server address `{address}`: {reason}")]
    Address { address: String, reason: String },
    #[error("cannot reach the server: {0}")]
    Unavailable(String),
    #[error("the server refused the request: {0}")]
    Rejected(String),
    #[error(transparent)]
    Config(#[from] oar_common::config::ConfigError),
    #[error(transparent)]
    Db(#[from] oar_common::db::DbError),
    #[error("invalid job request: {0}")]
    Validation(#[from] oar_common::types::ValidationError),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Ipc(#[from] oar_common::ipc::IpcError),
}

/// What the client daemon asks the server for.
#[async_trait]
pub trait ServerClient: Send + Sync + 'static {
    /// Submits a job and returns the id allocated by the server.
    async fn notify_new_job(&self, request: &JobRequest) -> Result<JobId, ClientError>;

    /// Current status of a job, or `None` if the server does not know it.
    async fn get_job(&self, job_id: JobId) -> Result<Option<JobSummary>, ClientError>;

    /// Lists the jobs known to the server, optionally filtered by state.
    async fn list_jobs(&self, state: Option<JobState>) -> Result<Vec<JobSummary>, ClientError>;
}

/// gRPC implementation talking to the unique server.
#[derive(Debug, Clone)]
pub struct GrpcServerClient {
    server_address: String,
    channel: Channel,
}

impl GrpcServerClient {
    /// Builds a lazy client for `server_address` (`host:port` or a full URI).
    pub fn new(server_address: impl Into<String>) -> Result<Self, ClientError> {
        let server_address = server_address.into();
        let channel =
            grpc::connect_lazy(&server_address).map_err(|error| ClientError::Address {
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
}

#[async_trait]
impl ServerClient for GrpcServerClient {
    async fn notify_new_job(&self, request: &JobRequest) -> Result<JobId, ClientError> {
        let mut client = SchedulerServiceClient::new(self.channel.clone());
        let response = client
            .notify_new_job(NotifyNewJobRequest {
                job: Some(convert::job_request_to_spec(request)),
            })
            .await
            .map_err(|status| ClientError::Unavailable(status.to_string()))?
            .into_inner();
        Ok(response.job_id)
    }

    async fn get_job(&self, job_id: JobId) -> Result<Option<JobSummary>, ClientError> {
        let mut client = AdminServiceClient::new(self.channel.clone());
        match client.get_job(GetJobRequest { job_id }).await {
            Ok(response) => {
                let status = response.into_inner();
                convert::status_to_summary(&status)
                    .map(Some)
                    .ok_or_else(|| {
                        ClientError::Rejected(format!("unknown job state `{}`", status.state))
                    })
            }
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(ClientError::Unavailable(status.to_string())),
        }
    }

    async fn list_jobs(&self, state: Option<JobState>) -> Result<Vec<JobSummary>, ClientError> {
        let mut client = AdminServiceClient::new(self.channel.clone());
        let response = client
            .list_jobs(ListJobsRequest {
                state: state.map(|state| state.to_string()),
            })
            .await
            .map_err(|status| ClientError::Unavailable(status.to_string()))?
            .into_inner();
        Ok(response
            .jobs
            .iter()
            .filter_map(convert::status_to_summary)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lazy_client_can_be_built_without_a_server() {
        let client = GrpcServerClient::new("127.0.0.1:6666").unwrap();
        assert_eq!(client.server_address(), "127.0.0.1:6666");
        assert!(GrpcServerClient::new("http://127.0.0.1:6666").is_ok());
        assert!(matches!(
            GrpcServerClient::new("http://"),
            Err(ClientError::Address { .. })
        ));
    }

    #[tokio::test]
    async fn calls_fail_cleanly_when_the_server_is_absent() {
        let client = GrpcServerClient::new("127.0.0.1:1").unwrap();
        let request = JobRequest::new(1000, "echo hi", 1, 60);
        assert!(matches!(
            client.notify_new_job(&request).await,
            Err(ClientError::Unavailable(_))
        ));
        assert!(matches!(
            client.get_job(1).await,
            Err(ClientError::Unavailable(_))
        ));
        assert!(matches!(
            client.list_jobs(None).await,
            Err(ClientError::Unavailable(_))
        ));
    }
}
