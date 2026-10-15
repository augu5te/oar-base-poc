//! Wire-level end-to-end tests: every gRPC service of the protocol, called by a
//! real generated client, over both supported transports (TCP and unix domain
//! sockets). This is what a future operator tool or a non-Rust client would use.

use std::time::{Duration, Instant};

use oar_common::config::Config;
use oar_common::grpc;
use oar_proto::admin_service_client::AdminServiceClient;
use oar_proto::compute_node_service_client::ComputeNodeServiceClient;
use oar_proto::node_callback_service_client::NodeCallbackServiceClient;
use oar_proto::scheduler_service_client::SchedulerServiceClient;
use oar_proto::*;
use tonic::transport::Channel;

fn spec(command: &str, nodes: u32, walltime_secs: u32) -> JobSpec {
    JobSpec {
        user_uid: 1000,
        command: command.into(),
        nodes,
        walltime_secs,
    }
}

fn server_config() -> Config {
    Config {
        server_hostname: "127.0.0.1".into(),
        server_port: 0,
        automaton_wait_secs: 1,
        scheduler_timeout_secs: 1,
        finaud_frequency_secs: 1,
        ..Config::default()
    }
}

/// Exercises the whole protocol against one channel (transport agnostic).
async fn exercise(channel: Channel) {
    // --- SchedulerService: the client daemon submits a job -------------------
    let mut scheduler = SchedulerServiceClient::new(channel.clone());
    let response = scheduler
        .notify_new_job(NotifyNewJobRequest {
            job: Some(spec("echo hi", 1, 60)),
        })
        .await
        .expect("NotifyNewJob")
        .into_inner();
    assert!(response.job_id > 0, "the server allocates the job id");

    // Invalid jobs are refused without creating anything.
    let refused = scheduler
        .notify_new_job(NotifyNewJobRequest {
            job: Some(spec("   ", 1, 60)),
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    let missing = scheduler
        .notify_new_job(NotifyNewJobRequest { job: None })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::InvalidArgument);

    // --- AdminService -------------------------------------------------------
    let mut admin = AdminServiceClient::new(channel.clone());
    let status = admin
        .get_job(GetJobRequest {
            job_id: response.job_id,
        })
        .await
        .expect("GetJob")
        .into_inner();
    assert_eq!(status.job_id, response.job_id);
    assert_eq!(status.state, "waiting");

    let unknown = admin.get_job(GetJobRequest { job_id: 4242 }).await;
    assert_eq!(unknown.unwrap_err().code(), tonic::Code::NotFound);

    let jobs = admin
        .list_jobs(ListJobsRequest { state: None })
        .await
        .expect("ListJobs")
        .into_inner()
        .jobs;
    assert_eq!(jobs.len(), 1, "only the valid job exists");

    let waiting = admin
        .list_jobs(ListJobsRequest {
            state: Some("waiting".into()),
        })
        .await
        .expect("ListJobs")
        .into_inner()
        .jobs;
    assert_eq!(waiting.len(), 1);
    let terminated = admin
        .list_jobs(ListJobsRequest {
            state: Some("terminated".into()),
        })
        .await
        .expect("ListJobs")
        .into_inner()
        .jobs;
    assert!(terminated.is_empty());
    assert!(
        admin
            .list_jobs(ListJobsRequest {
                state: Some("bogus".into())
            })
            .await
            .is_err()
    );

    // --- NodeCallbackService: registration, heartbeat, completion -----------
    let mut callbacks = NodeCallbackServiceClient::new(channel.clone());
    let ack = callbacks
        .register_node(RegisterNodeRequest {
            node_id: "n1".into(),
            address: "127.0.0.1:7001".into(),
        })
        .await
        .expect("RegisterNode")
        .into_inner();
    assert!(ack.ok, "{ack:?}");

    let invalid = callbacks
        .register_node(RegisterNodeRequest {
            node_id: String::new(),
            address: "127.0.0.1:7001".into(),
        })
        .await
        .expect("RegisterNode")
        .into_inner();
    assert!(!invalid.ok);

    let ack = callbacks
        .heartbeat(HeartbeatRequest {
            node_id: "n1".into(),
            running_jobs: 0,
        })
        .await
        .expect("Heartbeat")
        .into_inner();
    assert!(ack.ok);
    let ack = callbacks
        .heartbeat(HeartbeatRequest {
            node_id: "ghost".into(),
            running_jobs: 0,
        })
        .await
        .expect("Heartbeat")
        .into_inner();
    assert!(!ack.ok, "an unknown node is refused");

    // A completion notification is accepted only for a job that actually
    // started; here no usable node exists, so the job is still waiting.
    let state = admin
        .get_job(GetJobRequest {
            job_id: response.job_id,
        })
        .await
        .expect("GetJob")
        .into_inner()
        .state;
    let finishable = matches!(state.as_str(), "to_launch" | "launching" | "running");
    let ack = callbacks
        .job_finished(JobFinishedRequest {
            job_id: response.job_id,
            node_id: "n1".into(),
            exit_code: 0,
        })
        .await
        .expect("JobFinished")
        .into_inner();
    assert_eq!(
        ack.ok, finishable,
        "state `{state}` was reported as {ack:?}"
    );
    let ack = callbacks
        .job_finished(JobFinishedRequest {
            job_id: 4242,
            node_id: "n1".into(),
            exit_code: 0,
        })
        .await
        .expect("JobFinished")
        .into_inner();
    assert!(!ack.ok, "the completion of an unknown job is ignored");

    // --- ClusterStatus ------------------------------------------------------
    let cluster = admin
        .cluster_status(Empty {})
        .await
        .expect("ClusterStatus")
        .into_inner();
    assert_eq!(cluster.nodes_registered, 1);
    assert_eq!(cluster.nodes_alive, 1);
    assert!(cluster.jobs_finished + cluster.jobs_waiting + cluster.jobs_active >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_protocol_works_over_tcp() {
    let server = oar_server::start(server_config()).await.unwrap();
    let address = server.local_addr().unwrap().to_string();
    let channel = grpc::connect(&address).await.unwrap();

    exercise(channel).await;

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_protocol_works_over_a_unix_socket() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("oar/server.sock");

    let server = oar_server::start_unix(server_config(), &socket)
        .await
        .unwrap();
    assert!(server.endpoint().starts_with("unix:"));

    let channel = grpc::connect_unix(&socket).await.unwrap();
    exercise(channel).await;

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_serves_the_compute_node_service() {
    // The node has no server to talk to: registration fails and is retried, which
    // must not prevent it from answering the server's requests.
    let config = Config {
        server_hostname: "127.0.0.1".into(),
        server_port: 1, // nothing listens there
        node_id: "node-x".into(),
        node_address: "127.0.0.1:0".into(),
        node_heartbeat_secs: 1,
        simulated_job_duration_secs: 1,
        ..Config::default()
    };

    let node = oar_node::start(config).await.unwrap();
    let channel = grpc::connect(&node.address).await.unwrap();
    let mut client = ComputeNodeServiceClient::new(channel);

    let ping = client.ping(Empty {}).await.expect("Ping").into_inner();
    assert_eq!(ping.node_id, "node-x");
    assert_eq!(ping.running_jobs, 0);

    let ack = client
        .start_job(StartJobRequest {
            job_id: 11,
            user_uid: 1000,
            command: "echo hi".into(),
            nodes: vec![NodeRef {
                id: "node-x".into(),
                address: node.address.clone(),
            }],
            walltime_secs: 60,
        })
        .await
        .expect("StartJob")
        .into_inner();
    assert!(ack.ok, "{ack:?}");

    // The job is refused, not accepted, when the request is unusable.
    let ack = client
        .start_job(StartJobRequest {
            job_id: 12,
            user_uid: 1000,
            command: "   ".into(),
            nodes: vec![],
            walltime_secs: 60,
        })
        .await
        .expect("StartJob")
        .into_inner();
    assert!(!ack.ok);

    // The fake execution finishes on its own and the node becomes idle again.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let ping = client.ping(Empty {}).await.unwrap().into_inner();
        if ping.running_jobs == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "the job never finished");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    node.shutdown().await;
}
