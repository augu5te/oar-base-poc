//! Core domain types shared by client, server and compute nodes.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub type JobId = u64;
pub type NodeId = String;

/// Default walltime when the user does not give one (2 hours, like OAR).
pub const DEFAULT_WALLTIME_SECS: u32 = 2 * 3600;
/// Upper bound on the command length accepted by the client.
pub const MAX_COMMAND_BYTES: usize = 64 * 1024;

/// Seconds since the unix epoch, or 0 if the clock is before the epoch.
pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

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
    pub fn new(user_uid: u32, command: impl Into<String>, nodes: u32, walltime_secs: u32) -> Self {
        Self {
            user_uid,
            command: command.into(),
            nodes,
            walltime_secs,
        }
    }

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

/// Every job state, in life-cycle order.
pub const JOB_STATES: [JobState; 7] = [
    JobState::Waiting,
    JobState::ToLaunch,
    JobState::Launching,
    JobState::Running,
    JobState::Finishing,
    JobState::Terminated,
    JobState::Error,
];

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

    /// Lenient parser used for CLI / configuration input. Accepts the OAR3
    /// spellings (`toLaunch`, `toAckReservation`) as well as our own
    /// `snake_case`/`Display` ones, ignoring case.
    pub fn parse(input: &str) -> Option<JobState> {
        let normalized = input
            .trim()
            .to_ascii_lowercase()
            .replace(['_', ' ', '-'], "");
        JOB_STATES
            .into_iter()
            .find(|s| s.to_string().to_ascii_lowercase() == normalized)
    }

    /// The stable `snake_case` spelling used on the wire (JSON IPC and gRPC).
    ///
    /// It is exactly what `serde` produces for [`JobState`]
    /// (`#[serde(rename_all = "snake_case")]`), so both protocols agree.
    pub const fn wire_str(self) -> &'static str {
        match self {
            JobState::Waiting => "waiting",
            JobState::ToLaunch => "to_launch",
            JobState::Launching => "launching",
            JobState::Running => "running",
            JobState::Finishing => "finishing",
            JobState::Terminated => "terminated",
            JobState::Error => "error",
        }
    }
}

impl std::str::FromStr for JobState {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JobState::parse(s).ok_or_else(|| ValidationError(format!("unknown job state `{s}`")))
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid job transition {from} -> {to}")]
pub struct TransitionError {
    pub from: JobState,
    pub to: JobState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    pub request: JobRequest,
    pub state: JobState,
    /// Assigned nodes; the first one is the head node.
    pub assigned_nodes: Vec<NodeId>,
    pub exit_code: Option<i32>,
    /// Unix timestamp at which the job entered `Running`.
    pub started_at_unix: Option<u64>,
    /// Free-form status message (failure reason, ...).
    pub message: Option<String>,
}

impl Job {
    pub fn new(id: JobId, request: JobRequest) -> Self {
        Self {
            id,
            request,
            state: JobState::Waiting,
            assigned_nodes: Vec::new(),
            exit_code: None,
            started_at_unix: None,
            message: None,
        }
    }

    pub fn head_node(&self) -> Option<&NodeId> {
        self.assigned_nodes.first()
    }

    /// Applies a state change, refusing forbidden transitions.
    pub fn transition_to(&mut self, to: JobState) -> Result<(), TransitionError> {
        if !self.state.can_transition_to(to) {
            return Err(TransitionError {
                from: self.state,
                to,
            });
        }
        self.state = to;
        if to == JobState::Running {
            self.started_at_unix = Some(now_unix_secs());
        }
        Ok(())
    }

    /// Unix timestamp past which the job exceeded its walltime, if running.
    pub fn walltime_deadline_unix(&self) -> Option<u64> {
        self.started_at_unix
            .map(|start| start + u64::from(self.request.walltime_secs))
    }

    pub fn walltime_exceeded_at(&self, now_unix: u64) -> bool {
        self.walltime_deadline_unix()
            .is_some_and(|deadline| now_unix >= deadline)
    }
}

/// Filter used to query the (placeholder) database.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobFilter {
    pub state: Option<JobState>,
    pub user_uid: Option<u32>,
}

impl JobFilter {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn state(state: JobState) -> Self {
        Self {
            state: Some(state),
            ..Self::default()
        }
    }

    pub fn user(user_uid: u32) -> Self {
        Self {
            user_uid: Some(user_uid),
            ..Self::default()
        }
    }

    pub fn matches(&self, job: &Job) -> bool {
        self.state.is_none_or(|s| job.state == s)
            && self.user_uid.is_none_or(|u| job.request.user_uid == u)
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
        Self {
            id: id.into(),
            address: address.into(),
            alive: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> JobRequest {
        JobRequest {
            user_uid: 1000,
            command: "echo hi".into(),
            nodes: 1,
            walltime_secs: 60,
        }
    }

    fn job() -> Job {
        Job::new(1, req())
    }

    #[test]
    fn the_wire_spelling_is_snake_case_and_matches_serde() {
        for state in JOB_STATES {
            // The JSON IPC protocol and the gRPC protocol must agree.
            assert_eq!(serde_json::to_value(state).unwrap(), state.wire_str());
            assert_eq!(
                serde_json::to_string(&state).unwrap(),
                format!("\"{}\"", state.wire_str())
            );
            // ... and the wire spelling must round-trip through the lenient parser.
            assert_eq!(JobState::parse(state.wire_str()), Some(state));
            assert_eq!(JobState::parse(&state.to_string()), Some(state));
        }
        assert_eq!(JOB_STATES.len(), 7);
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
        let mut job = job();
        job.assigned_nodes = vec!["n2".into(), "n1".into()];
        assert_eq!(job.head_node().map(String::as_str), Some("n2"));
        assert!(Job::new(2, req()).head_node().is_none());
    }

    #[test]
    fn transition_to_records_start_time() {
        let mut job = job();
        assert!(job.transition_to(JobState::ToLaunch).is_ok());
        assert_eq!(job.started_at_unix, None);
        assert!(job.transition_to(JobState::Launching).is_ok());
        assert!(job.transition_to(JobState::Running).is_ok());
        assert!(job.started_at_unix.is_some());
        assert!(job.walltime_deadline_unix().is_some());
    }

    #[test]
    fn transition_to_refuses_forbidden_change() {
        let mut job = job();
        assert_eq!(
            job.transition_to(JobState::Running),
            Err(TransitionError {
                from: JobState::Waiting,
                to: JobState::Running
            })
        );
        assert_eq!(job.state, JobState::Waiting);
    }

    #[test]
    fn walltime_exceeded_is_detected() {
        let mut job = job();
        job.transition_to(JobState::ToLaunch).unwrap();
        job.transition_to(JobState::Launching).unwrap();
        job.transition_to(JobState::Running).unwrap();
        let start = job.started_at_unix.unwrap();
        assert!(!job.walltime_exceeded_at(start));
        assert!(!job.walltime_exceeded_at(start + 59));
        assert!(job.walltime_exceeded_at(start + 60));
    }

    #[test]
    fn job_state_parsing_is_lenient() {
        assert_eq!(JobState::parse("toLaunch"), Some(JobState::ToLaunch));
        assert_eq!(JobState::parse("to_launch"), Some(JobState::ToLaunch));
        assert_eq!(JobState::parse("RUNNING"), Some(JobState::Running));
        assert_eq!(JobState::parse("nope"), None);
        assert_eq!(
            "terminated".parse::<JobState>().unwrap(),
            JobState::Terminated
        );
    }

    #[test]
    fn job_filter_matches() {
        let job = job();
        assert!(JobFilter::all().matches(&job));
        assert!(JobFilter::state(JobState::Waiting).matches(&job));
        assert!(!JobFilter::state(JobState::Running).matches(&job));
        assert!(JobFilter::user(1000).matches(&job));
        assert!(!JobFilter::user(1).matches(&job));
    }
}
