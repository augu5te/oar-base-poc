//! Conversions between the domain types and the gRPC wire messages.
//!
//! The domain never depends on `prost` types directly: only this module and the
//! gRPC service implementations know about the generated code.

use crate::ipc::JobSummary;
use crate::types::{Job, JobRequest, JobState, NodeId};
use oar_proto as pb;

/// Parses a wire state string; unknown values are reported as `None`.
pub fn job_state_from_wire(state: &str) -> Option<JobState> {
    JobState::parse(state)
}

/// Renders a job state in its stable wire spelling (`snake_case`), the same one
/// the JSON IPC protocol uses.
pub fn job_state_to_wire(state: JobState) -> String {
    state.wire_str().to_string()
}

pub fn job_request_to_spec(request: &JobRequest) -> pb::JobSpec {
    pb::JobSpec {
        user_uid: request.user_uid,
        command: request.command.clone(),
        nodes: request.nodes,
        walltime_secs: request.walltime_secs,
    }
}

pub fn spec_to_job_request(spec: &pb::JobSpec) -> JobRequest {
    JobRequest {
        user_uid: spec.user_uid,
        command: spec.command.clone(),
        nodes: spec.nodes,
        walltime_secs: spec.walltime_secs,
    }
}

pub fn job_to_status(job: &Job, address_of: impl Fn(&NodeId) -> Option<String>) -> pb::JobStatus {
    pb::JobStatus {
        job_id: job.id,
        state: job_state_to_wire(job.state),
        nodes: job
            .assigned_nodes
            .iter()
            .map(|id| pb::NodeRef {
                id: id.clone(),
                address: address_of(id).unwrap_or_default(),
            })
            .collect(),
        exit_code: job.exit_code,
        message: job.message.clone().unwrap_or_default(),
    }
}

pub fn job_to_summary(job: &Job) -> JobSummary {
    JobSummary {
        job_id: job.id,
        state: job.state,
        nodes: job.assigned_nodes.clone(),
        exit_code: job.exit_code,
    }
}

pub fn status_to_summary(status: &pb::JobStatus) -> Option<JobSummary> {
    Some(JobSummary {
        job_id: status.job_id,
        state: job_state_from_wire(&status.state)?,
        nodes: status.nodes.iter().map(|node| node.id.clone()).collect(),
        exit_code: status.exit_code,
    })
}

pub fn summary_to_status(summary: &JobSummary) -> pb::JobStatus {
    pb::JobStatus {
        job_id: summary.job_id,
        state: job_state_to_wire(summary.state),
        nodes: summary
            .nodes
            .iter()
            .map(|id| pb::NodeRef {
                id: id.clone(),
                address: String::new(),
            })
            .collect(),
        exit_code: summary.exit_code,
        message: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> JobRequest {
        JobRequest::new(1000, "echo hi", 2, 3600)
    }

    fn running_job() -> Job {
        let mut job = Job::new(7, request());
        job.assigned_nodes = vec!["n1".into(), "n2".into()];
        job.exit_code = Some(3);
        job.message = Some("done".into());
        job.transition_to(JobState::ToLaunch).unwrap();
        job.transition_to(JobState::Launching).unwrap();
        job.transition_to(JobState::Running).unwrap();
        job
    }

    #[test]
    fn job_request_roundtrips_through_the_wire() {
        let spec = job_request_to_spec(&request());
        assert_eq!(spec.user_uid, 1000);
        assert_eq!(spec.nodes, 2);
        assert_eq!(spec_to_job_request(&spec), request());
    }

    #[test]
    fn job_status_carries_nodes_and_exit_code() {
        let job = running_job();
        let status = job_to_status(&job, |id| {
            (id == "n1").then(|| "127.0.0.1:7001".to_string())
        });
        assert_eq!(status.job_id, 7);
        assert_eq!(status.state, "running");
        assert_eq!(status.nodes.len(), 2);
        assert_eq!(status.nodes[0].address, "127.0.0.1:7001");
        assert_eq!(status.nodes[1].address, "");
        assert_eq!(status.exit_code, Some(3));
        assert_eq!(status.message, "done");

        let summary = status_to_summary(&status).unwrap();
        assert_eq!(summary.job_id, 7);
        assert_eq!(summary.state, JobState::Running);
        assert_eq!(summary.nodes, ["n1", "n2"]);
        assert_eq!(summary.exit_code, Some(3));
        assert_eq!(job_to_summary(&job), summary);
        assert_eq!(summary_to_status(&summary).state, "running");
    }

    #[test]
    fn unknown_wire_state_is_rejected() {
        let status = pb::JobStatus {
            job_id: 1,
            state: "flying".into(),
            ..Default::default()
        };
        assert!(status_to_summary(&status).is_none());
    }
}
