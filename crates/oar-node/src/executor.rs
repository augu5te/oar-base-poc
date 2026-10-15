//! Job execution on the compute node.
//!
//! Executions are **faked** for now: [`FakeExecutor`] waits for a configurable
//! duration and derives a deterministic exit code from the command. The real
//! `oarexec` / cpuset implementation will provide another [`Executor`] without
//! touching the gRPC service.

use std::time::Duration;

use async_trait::async_trait;
use oar_common::types::{JobId, NodeId};

/// The part of a `StartJobRequest` the executor needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobExecution {
    pub job_id: JobId,
    pub user_uid: u32,
    pub command: String,
    /// All nodes of the job, head node first.
    pub nodes: Vec<NodeId>,
    pub walltime_secs: u32,
}

impl JobExecution {
    /// The head node, i.e. the node running the job's command.
    pub fn head_node(&self) -> Option<&NodeId> {
        self.nodes.first()
    }
}

/// Runs a job (or, for now, pretends to) and reports its exit code.
#[async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn run(&self, job: &JobExecution) -> i32;
}

/// Simulated executor: sleeps, then returns a deterministic exit code.
///
/// The simulated duration is [`FakeExecutor::default_duration`], capped by the
/// job walltime. The exit code is derived from the command by
/// [`FakeExecutor::exit_code_for`].
#[derive(Debug, Clone)]
pub struct FakeExecutor {
    default_duration: Duration,
}

impl Default for FakeExecutor {
    fn default() -> Self {
        Self::new(Duration::from_secs(2))
    }
}

impl FakeExecutor {
    pub fn new(default_duration: Duration) -> Self {
        Self { default_duration }
    }

    /// Duration a job will "run" for: the default, capped by the walltime.
    pub fn duration_for(&self, job: &JobExecution) -> Duration {
        let walltime = Duration::from_secs(u64::from(job.walltime_secs));
        if walltime.is_zero() {
            self.default_duration
        } else {
            self.default_duration.min(walltime)
        }
    }

    /// Deterministic fake exit code:
    ///
    /// * `exit=<n>` anywhere in the command -> `n`;
    /// * a command containing `fail` -> `1`;
    /// * anything else -> `0`.
    pub fn exit_code_for(command: &str) -> i32 {
        if let Some(code) = command
            .split_whitespace()
            .find_map(|token| token.strip_prefix("exit="))
            && let Ok(code) = code.parse::<i32>()
        {
            return code;
        }
        if command.to_ascii_lowercase().contains("fail") {
            1
        } else {
            0
        }
    }
}

#[async_trait]
impl Executor for FakeExecutor {
    async fn run(&self, job: &JobExecution) -> i32 {
        let duration = self.duration_for(job);
        tracing::info!(
            job_id = job.job_id,
            user_uid = job.user_uid,
            ?duration,
            command = %job.command,
            "simulating job execution"
        );
        if !duration.is_zero() {
            tokio::time::sleep(duration).await;
        }
        let exit_code = Self::exit_code_for(&job.command);
        tracing::info!(
            job_id = job.job_id,
            exit_code,
            "simulated execution finished"
        );
        exit_code
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execution(command: &str, walltime_secs: u32) -> JobExecution {
        JobExecution {
            job_id: 1,
            user_uid: 1000,
            command: command.into(),
            nodes: vec!["n1".into(), "n2".into()],
            walltime_secs,
        }
    }

    #[test]
    fn exit_codes_are_derived_from_the_command() {
        assert_eq!(FakeExecutor::exit_code_for("echo hi"), 0);
        assert_eq!(FakeExecutor::exit_code_for("please fail now"), 1);
        assert_eq!(FakeExecutor::exit_code_for("echo hi exit=42"), 42);
        assert_eq!(FakeExecutor::exit_code_for("exit=-1"), -1);
        assert_eq!(FakeExecutor::exit_code_for("exit=notanumber"), 0);
    }

    #[test]
    fn duration_is_capped_by_the_walltime() {
        let executor = FakeExecutor::new(Duration::from_secs(10));
        assert_eq!(
            executor.duration_for(&execution("x", 3)),
            Duration::from_secs(3)
        );
        assert_eq!(
            executor.duration_for(&execution("x", 60)),
            Duration::from_secs(10)
        );
        assert_eq!(
            executor.duration_for(&execution("x", 0)),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn head_node_is_the_first_one() {
        assert_eq!(
            execution("x", 1).head_node().map(String::as_str),
            Some("n1")
        );
    }

    #[tokio::test]
    async fn fake_execution_returns_the_derived_code() {
        let executor = FakeExecutor::new(Duration::ZERO);
        assert_eq!(executor.run(&execution("echo hi", 60)).await, 0);
        assert_eq!(executor.run(&execution("exit=7", 60)).await, 7);
    }
}
