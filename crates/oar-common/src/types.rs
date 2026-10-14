//! Core domain types shared by client, server and compute nodes.

use serde::{Deserialize, Serialize};

pub type JobId = u64;
pub type NodeId = String;

/// Default walltime when the user does not give one (2 hours, like OAR).
pub const DEFAULT_WALLTIME_SECS: u32 = 2 * 3600;
/// Upper bound on the command length accepted by the client.
pub const MAX_COMMAND_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid job request: {0}")]
pub struct ValidationError(pub String);

/// What a user asks for when submitting a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRequest {
    pub user_uid: u32,
    pub command: String,
    pub nodes: u32,
    pub walltime_secs: u32,
}

impl JobRequest {
    /// Placeholder for the real admission rules.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.command.trim().is_empty() {
            return Err(ValidationError("empty command".into()));
        }
        if self.command.len() > MAX_COMMAND_BYTES {
            return Err(ValidationError(format!(
                "command too long ({} bytes, max {MAX_COMMAND_BYTES})",
                self.command.len()
            )));
        }
        if self.nodes == 0 {
            return Err(ValidationError("at least one node is required".into()));
        }
        if self.walltime_secs == 0 {
            return Err(ValidationError("walltime must be greater than zero".into()));
        }
        Ok(())
    }
}

/// Life cycle of a job.
///
/// ```text
/// Waiting -> ToLaunch -> Launching -> Running -> Finishing -> Terminated
///                 \___________\___________\___________\____-> Error
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Waiting,
    ToLaunch,
    Launching,
    Running,
    Finishing,
    Terminated,
    Error,
}

impl JobState {
    pub fn is_final(self) -> bool {
        matches!(self, JobState::Terminated | JobState::Error)
    }

    /// True while the job holds (or is about to hold) resources.
    pub fn is_active(self) -> bool {
        matches!(
            self,
            JobState::ToLaunch | JobState::Launching | JobState::Running | JobState::Finishing
        )
    }

    pub fn can_transition_to(self, to: JobState) -> bool {
        use JobState::*;
        matches!(
            (self, to),
            (Waiting, ToLaunch)
                | (ToLaunch, Launching)
                | (Launching, Running)
                | (Launching, Finishing) // job finished before the server saw the launch ack
                | (Running, Finishing)
                | (Finishing, Terminated)
                | (Waiting | ToLaunch | Launching | Running | Finishing, Error)
        )
    }
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            JobState::Waiting => "Waiting",
            JobState::ToLaunch => "ToLaunch",
            JobState::Launching => "Launching",
            JobState::Running => "Running",
            JobState::Finishing => "Finishing",
            JobState::Terminated => "Terminated",
            JobState::Error => "Error",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    pub request: JobRequest,
    pub state: JobState,
    /// Assigned nodes; the first one is the head node.
    pub assigned_nodes: Vec<NodeId>,
    pub exit_code: Option<i32>,
}

impl Job {
    pub fn head_node(&self) -> Option<&NodeId> {
        self.assigned_nodes.first()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub id: NodeId,
    /// `host:port` of the node's gRPC endpoint.
    pub address: String,
    pub alive: bool,
}

impl NodeInfo {
    pub fn new(id: impl Into<String>, address: impl Into<String>) -> Self {
        Self { id: id.into(), address: address.into(), alive: true }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> JobRequest {
        JobRequest { user_uid: 1000, command: "echo hi".into(), nodes: 1, walltime_secs: 60 }
    }

    #[test]
    fn valid_request_passes() {
        assert!(req().validate().is_ok());
    }

    #[test]
    fn invalid_requests_are_rejected() {
        let mut r = req();
        r.command = "   ".into();
        assert!(r.validate().is_err());

        let mut r = req();
        r.nodes = 0;
        assert!(r.validate().is_err());

        let mut r = req();
        r.walltime_secs = 0;
        assert!(r.validate().is_err());

        let mut r = req();
        r.command = "x".repeat(MAX_COMMAND_BYTES + 1);
        assert!(r.validate().is_err());
    }

    #[test]
    fn nominal_transitions_are_allowed() {
        use JobState::*;
        let path = [Waiting, ToLaunch, Launching, Running, Finishing, Terminated];
        for w in path.windows(2) {
            assert!(w[0].can_transition_to(w[1]), "{} -> {}", w[0], w[1]);
        }
    }

    #[test]
    fn forbidden_transitions_are_refused() {
        use JobState::*;
        assert!(!Waiting.can_transition_to(Running));
        assert!(!Terminated.can_transition_to(Error));
        assert!(!Error.can_transition_to(Waiting));
        assert!(!Running.can_transition_to(Launching));
    }

    #[test]
    fn early_finish_is_allowed_from_launching() {
        assert!(JobState::Launching.can_transition_to(JobState::Finishing));
    }

    #[test]
    fn final_and_active_states() {
        assert!(JobState::Terminated.is_final());
        assert!(JobState::Error.is_final());
        assert!(!JobState::Waiting.is_final());
        assert!(!JobState::Waiting.is_active());
        assert!(JobState::Running.is_active());
        assert!(!JobState::Terminated.is_active());
    }

    #[test]
    fn head_node_is_first_assigned() {
        let job = Job {
            id: 1,
            request: req(),
            state: JobState::ToLaunch,
            assigned_nodes: vec!["n2".into(), "n1".into()],
            exit_code: None,
        };
        assert_eq!(job.head_node().map(String::as_str), Some("n2"));
    }
}
