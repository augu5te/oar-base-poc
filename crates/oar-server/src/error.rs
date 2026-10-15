//! Error type shared by the server engine, the automaton and the gRPC services.

use oar_common::config::ConfigError;
use oar_common::db::DbError;
use oar_common::grpc::GrpcError;
use oar_common::types::{JobId, TransitionError, ValidationError};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Grpc(#[from] GrpcError),
    #[error(transparent)]
    Transition(#[from] TransitionError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("job {0} not found")]
    JobNotFound(JobId),
    #[error("the {step} worker task is gone")]
    WorkerGone { step: &'static str },
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("task failed: {0}")]
    Task(String),
}

impl ServerError {
    /// Maps the error to the gRPC status returned to the caller.
    pub fn to_status(&self) -> tonic::Status {
        use tonic::Code;
        match self {
            ServerError::Validation(_) => tonic::Status::invalid_argument(self.to_string()),
            ServerError::JobNotFound(_) => tonic::Status::not_found(self.to_string()),
            ServerError::Db(DbError::NotFound(_)) => tonic::Status::not_found(self.to_string()),
            ServerError::Db(DbError::AlreadyExists { .. }) => {
                tonic::Status::already_exists(self.to_string())
            }
            _ => {
                tracing::error!(error = %self, "internal server error");
                tonic::Status::new(Code::Internal, self.to_string())
            }
        }
    }
}
