//! The gRPC services exposed by the server:
//!
//! * `SchedulerService` — client daemons notify new submissions;
//! * `NodeCallbackService` — compute nodes register, heartbeat and report job
//!   completions;
//! * `AdminService` — cluster introspection for operators and the CLI.
//!
//! Every handler only validates its input, updates the placeholder database /
//! registry through the [`Engine`], and lets the automaton do the actual work.

use std::sync::Arc;

use oar_common::convert;
use oar_common::types::{JobFilter, JobState};
use oar_proto::admin_service_server::AdminService;
use oar_proto::node_callback_service_server::NodeCallbackService;
use oar_proto::scheduler_service_server::SchedulerService;
use oar_proto::{
    Ack, ClusterStatusResponse, Empty, GetJobRequest, HeartbeatRequest, JobFinishedRequest,
    JobStatus, ListJobsRequest, ListJobsResponse, NotifyNewJobRequest, NotifyNewJobResponse,
    RegisterNodeRequest,
};
use tonic::{Request, Response, Status};

use crate::engine::Engine;

/// `SchedulerService`: new job notifications from the client daemons.
#[derive(Debug, Clone)]
pub struct SchedulerGrpc {
    engine: Arc<Engine>,
}

impl SchedulerGrpc {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }
}

#[tonic::async_trait]
impl SchedulerService for SchedulerGrpc {
    async fn notify_new_job(
        &self,
        request: Request<NotifyNewJobRequest>,
    ) -> Result<Response<NotifyNewJobResponse>, Status> {
        let spec = request
            .into_inner()
            .job
            .ok_or_else(|| Status::invalid_argument("missing job specification"))?;
        let job_id = self
            .engine
            .submit_job(convert::spec_to_job_request(&spec))
            .await
            .map_err(|error| error.to_status())?;
        Ok(Response::new(NotifyNewJobResponse { job_id }))
    }
}

/// `NodeCallbackService`: registration, heartbeat and job completion.
#[derive(Debug, Clone)]
pub struct NodeCallbackGrpc {
    engine: Arc<Engine>,
}

impl NodeCallbackGrpc {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }
}

#[tonic::async_trait]
impl NodeCallbackService for NodeCallbackGrpc {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        if !self
            .engine
            .register_node(&request.node_id, &request.address)
        {
            return Ok(Response::new(Ack {
                ok: false,
                message: "node_id and address must not be empty".into(),
            }));
        }
        Ok(Response::new(Ack {
            ok: true,
            message: format!("node {} registered", request.node_id),
        }))
    }

    async fn heartbeat(&self, request: Request<HeartbeatRequest>) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        match self
            .engine
            .heartbeat(&request.node_id, request.running_jobs)
        {
            Some(_) => Ok(Response::new(Ack {
                ok: true,
                message: String::new(),
            })),
            None => Ok(Response::new(Ack {
                ok: false,
                message: format!("unknown node {}", request.node_id),
            })),
        }
    }

    async fn job_finished(
        &self,
        request: Request<JobFinishedRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        let accepted = self
            .engine
            .finish_job(request.job_id, &request.node_id, request.exit_code)
            .await
            .map_err(|error| error.to_status())?;
        Ok(Response::new(Ack {
            ok: accepted,
            message: if accepted {
                String::new()
            } else {
                format!("job {} unknown or already finished", request.job_id)
            },
        }))
    }
}

/// `AdminService`: read-only cluster introspection.
#[derive(Debug, Clone)]
pub struct AdminGrpc {
    engine: Arc<Engine>,
}

impl AdminGrpc {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    fn status_of(&self, job: &oar_common::types::Job) -> JobStatus {
        let registry = Arc::clone(self.engine.registry());
        convert::job_to_status(job, move |id| registry.get(id).map(|node| node.address))
    }
}

#[tonic::async_trait]
impl AdminService for AdminGrpc {
    async fn get_job(
        &self,
        request: Request<GetJobRequest>,
    ) -> Result<Response<JobStatus>, Status> {
        let job_id = request.into_inner().job_id;
        let job = self
            .engine
            .job(job_id)
            .await
            .map_err(|error| error.to_status())?
            .ok_or_else(|| Status::not_found(format!("job {job_id} not found")))?;
        Ok(Response::new(self.status_of(&job)))
    }

    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let state = match request.into_inner().state {
            Some(state) if !state.trim().is_empty() => Some(
                JobState::parse(&state)
                    .ok_or_else(|| Status::invalid_argument(format!("unknown state `{state}`")))?,
            ),
            _ => None,
        };
        let filter = JobFilter {
            state,
            user_uid: None,
        };
        let jobs = self
            .engine
            .jobs(&filter)
            .await
            .map_err(|error| error.to_status())?;
        Ok(Response::new(ListJobsResponse {
            jobs: jobs.iter().map(|job| self.status_of(job)).collect(),
        }))
    }

    async fn cluster_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let status = self
            .engine
            .cluster_status()
            .await
            .map_err(|error| error.to_status())?;
        Ok(Response::new(ClusterStatusResponse {
            nodes_registered: status.nodes_registered,
            nodes_alive: status.nodes_alive,
            jobs_waiting: status.jobs_waiting,
            jobs_active: status.jobs_active,
            jobs_finished: status.jobs_finished,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use oar_common::types::JobRequest;
    use oar_proto::JobSpec;

    fn engine() -> (
        Arc<Engine>,
        tokio::sync::mpsc::UnboundedReceiver<crate::automaton::Event>,
    ) {
        Engine::builder().build()
    }

    fn spec() -> JobSpec {
        JobSpec {
            user_uid: 1000,
            command: "echo hi".into(),
            nodes: 1,
            walltime_secs: 60,
        }
    }

    #[tokio::test]
    async fn notify_new_job_allocates_an_id_and_queues_the_event() {
        let (engine, mut events) = engine();
        let service = SchedulerGrpc::new(engine.clone());

        let response = service
            .notify_new_job(Request::new(NotifyNewJobRequest { job: Some(spec()) }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.job_id, 1);
        assert!(matches!(
            events.try_recv(),
            Ok(crate::automaton::Event::NewJob { job_id: 1 })
        ));

        // A missing specification is an invalid argument.
        let error = service
            .notify_new_job(Request::new(NotifyNewJobRequest { job: None }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        // An invalid command too.
        let mut bad = spec();
        bad.command = "  ".into();
        let error = service
            .notify_new_job(Request::new(NotifyNewJobRequest { job: Some(bad) }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn registration_heartbeat_and_finish_callbacks() {
        let (engine, mut events) = engine();
        let service = NodeCallbackGrpc::new(engine.clone());

        let ack = service
            .register_node(Request::new(RegisterNodeRequest {
                node_id: "n1".into(),
                address: "127.0.0.1:7001".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.ok);
        assert!(matches!(
            events.try_recv(),
            Ok(crate::automaton::Event::CheckNodes)
        ));

        let ack = service
            .register_node(Request::new(RegisterNodeRequest {
                node_id: "".into(),
                address: "127.0.0.1:7001".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!ack.ok);

        let ack = service
            .heartbeat(Request::new(HeartbeatRequest {
                node_id: "n1".into(),
                running_jobs: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.ok);

        let ack = service
            .heartbeat(Request::new(HeartbeatRequest {
                node_id: "nope".into(),
                running_jobs: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!ack.ok);

        // Unknown job: acknowledged but refused.
        let ack = service
            .job_finished(Request::new(JobFinishedRequest {
                job_id: 42,
                node_id: "n1".into(),
                exit_code: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!ack.ok);

        // Known running job.
        let job_id = engine
            .submit_job(JobRequest::new(1000, "echo hi", 1, 60))
            .await
            .unwrap();
        engine.registry().upsert("n1", "127.0.0.1:7001");
        let mut job = engine.job(job_id).await.unwrap().unwrap();
        job.assigned_nodes = vec!["n1".into()];
        job.transition_to(JobState::ToLaunch).unwrap();
        job.transition_to(JobState::Launching).unwrap();
        job.transition_to(JobState::Running).unwrap();
        engine.database().update_job(&job).await.unwrap();

        let ack = service
            .job_finished(Request::new(JobFinishedRequest {
                job_id,
                node_id: "n1".into(),
                exit_code: 7,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.ok);
        assert_eq!(
            engine.job(job_id).await.unwrap().unwrap().exit_code,
            Some(7)
        );
    }

    #[tokio::test]
    async fn admin_service_reports_jobs_and_cluster_status() {
        let (engine, _events) = engine();
        engine.register_node("n1", "127.0.0.1:7001");
        let job_id = engine
            .submit_job(JobRequest::new(1000, "echo hi", 1, 60))
            .await
            .unwrap();
        let service = AdminGrpc::new(engine.clone());

        let status = service
            .get_job(Request::new(GetJobRequest { job_id }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(status.job_id, job_id);
        assert_eq!(status.state, "waiting");

        let error = service
            .get_job(Request::new(GetJobRequest { job_id: 999 }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);

        let listed = service
            .list_jobs(Request::new(ListJobsRequest { state: None }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(listed.jobs.len(), 1);

        let filtered = service
            .list_jobs(Request::new(ListJobsRequest {
                state: Some("running".into()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(filtered.jobs.is_empty());

        let error = service
            .list_jobs(Request::new(ListJobsRequest {
                state: Some("flying".into()),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        let status = service
            .cluster_status(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(status.nodes_registered, 1);
        assert_eq!(status.nodes_alive, 1);
        assert_eq!(status.jobs_waiting, 1);
    }
}
