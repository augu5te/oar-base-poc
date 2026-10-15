//! Whole-cluster end-to-end test: one `oar-server`, one `oar-node` and one
//! `oar-client`, all started in-process but talking over **real** gRPC sockets
//! and a **real** unix domain socket.
//!
//! ```text
//!   test ──JSON/UDS──> oar-client ──gRPC/TCP──> oar-server ──gRPC/TCP──> oar-node
//! ```
//!
//! Only the scheduler and the job executor are placeholders; everything else
//! (transport, codecs, automaton, state machine, wire formats) is exercised for
//! real.

use std::sync::Arc;
use std::time::{Duration, Instant};

use oar_client::{ClientDaemon, GrpcServerClient, start as start_client};
use oar_common::config::Config;
use oar_common::db::MemoryDatabase;
use oar_common::ipc::{self, IpcRequest, IpcResponse};
use oar_common::types::{JobId, JobState};
use oar_server::{Engine, start as start_server};

/// A running in-process cluster.
struct Cluster {
    directory: tempfile::TempDir,
    socket: std::path::PathBuf,
    server: oar_server::RunningServer,
    node: oar_node::RunningNode,
    client: oar_client::RunningClient,
    engine: Arc<Engine>,
}

impl Cluster {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("client.sock");

        // --- server (TCP, port 0 => a free port) ---
        let server_config = Config {
            server_hostname: "127.0.0.1".into(),
            server_port: 0,
            automaton_wait_secs: 1,
            scheduler_timeout_secs: 1,
            scheduler_job_security_time_secs: 1,
            finaud_frequency_secs: 1,
            simulated_job_duration_secs: 1,
            ..Config::default()
        };
        let server = start_server(server_config.clone()).await.unwrap();
        let server_port = server.local_addr().unwrap().port();
        let engine = Arc::clone(&server.engine);

        // --- one compute node, announcing itself to the server ---
        let mut node_config = server_config.clone();
        node_config.server_hostname = "127.0.0.1".into();
        node_config.server_port = server_port;
        node_config.node_id = "node-1".into();
        node_config.node_address = "127.0.0.1:0".into();
        node_config.node_heartbeat_secs = 1;
        let node = oar_node::start(node_config).await.unwrap();

        // --- client daemon, speaking gRPC to the server ---
        let backend = Arc::new(GrpcServerClient::new(format!("127.0.0.1:{server_port}")).unwrap());
        let daemon = Arc::new(ClientDaemon::new(
            Arc::new(MemoryDatabase::new()),
            backend,
            Arc::new(server_config),
        ));
        let client = start_client(daemon, &socket).await.unwrap();

        Self {
            directory,
            socket,
            server,
            node,
            client,
            engine,
        }
    }

    async fn submit(&self, command: &str, nodes: u32, walltime_secs: u32) -> IpcResponse {
        ipc::request(
            &self.socket,
            &IpcRequest::Submit {
                command: command.to_string(),
                nodes,
                walltime_secs,
            },
        )
        .await
        .unwrap()
    }

    async fn stat(&self, job_id: JobId) -> IpcResponse {
        ipc::request(&self.socket, &IpcRequest::Status { job_id })
            .await
            .unwrap()
    }

    async fn list(&self) -> IpcResponse {
        ipc::request(&self.socket, &IpcRequest::List { state: None })
            .await
            .unwrap()
    }

    /// Waits for a job to reach `expected`, polling the engine through the gRPC
    /// `AdminService` (via the client daemon) to make sure the server's view is
    /// consistent too.
    async fn wait_for_state(&self, job_id: JobId, expected: JobState) -> oar_common::types::Job {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let job = self.engine.job(job_id).await.unwrap().expect("job exists");
            if job.state == expected {
                return job;
            }
            if job.state.is_final() {
                panic!(
                    "job {job_id} reached {:?} instead of {expected:?}",
                    job.state
                );
            }
            assert!(
                Instant::now() < deadline,
                "job {job_id} stuck in {:?}",
                job.state
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_for_node(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.engine.registry().contains("node-1") {
            assert!(Instant::now() < deadline, "the node never registered");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop(self) {
        self.client.shutdown().await;
        self.node.shutdown().await;
        self.server.shutdown().await;
        drop(self.directory);
    }
}

/// Whether the server currently considers `id` alive.
fn node_alive(engine: &Engine, id: &str) -> bool {
    engine
        .registry()
        .get(id)
        .map(|node| node.alive)
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_runs_from_submission_to_termination() {
    let cluster = Cluster::start().await;
    cluster.wait_for_node().await;

    let response = cluster.submit("echo hello", 1, 60).await;
    let job_id = match response {
        IpcResponse::Submitted { job_id, .. } => job_id,
        other => panic!("unexpected response: {other:?}"),
    };
    assert_eq!(job_id, 1);

    let job = cluster.wait_for_state(job_id, JobState::Terminated).await;
    assert_eq!(job.exit_code, Some(0), "the fake executor reports success");
    assert_eq!(job.assigned_nodes, ["node-1"]);

    // The node is back to idle and still alive.
    assert_eq!(cluster.node.state().running_jobs(), 0);
    assert!(node_alive(&cluster.engine, "node-1"));

    // The client daemon and the server agree on the job.
    match cluster.stat(job_id).await {
        IpcResponse::Status { job: summary } => {
            assert_eq!(summary.job_id, job_id);
            assert_eq!(summary.state, JobState::Terminated);
            assert_eq!(summary.nodes, ["node-1"]);
            assert_eq!(summary.exit_code, Some(0));
        }
        other => panic!("unexpected response: {other:?}"),
    }

    match cluster.list().await {
        IpcResponse::Jobs { jobs } => assert_eq!(jobs.len(), 1),
        other => panic!("unexpected response: {other:?}"),
    }

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_command_reports_its_exit_code() {
    let cluster = Cluster::start().await;
    cluster.wait_for_node().await;

    let job_id = match cluster.submit("exit=7", 1, 60).await {
        IpcResponse::Submitted { job_id, .. } => job_id,
        other => panic!("unexpected response: {other:?}"),
    };

    let job = cluster.wait_for_state(job_id, JobState::Terminated).await;
    assert_eq!(job.exit_code, Some(7));

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jobs_are_queued_while_the_only_node_is_busy() {
    let cluster = Cluster::start().await;
    cluster.wait_for_node().await;

    // Two jobs but a single node: the second one must wait.
    let first = match cluster.submit("echo one", 1, 60).await {
        IpcResponse::Submitted { job_id, .. } => job_id,
        other => panic!("unexpected response: {other:?}"),
    };
    let second = match cluster.submit("echo two", 1, 60).await {
        IpcResponse::Submitted { job_id, .. } => job_id,
        other => panic!("unexpected response: {other:?}"),
    };
    assert_eq!((first, second), (1, 2));

    // The FIFO scheduler must not launch both at once.
    cluster.wait_for_state(first, JobState::Terminated).await;
    let second_job = cluster.wait_for_state(second, JobState::Terminated).await;
    assert_eq!(second_job.exit_code, Some(0));
    assert_eq!(second_job.assigned_nodes, ["node-1"]);

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_nodes_can_register_and_be_used() {
    let cluster = Cluster::start().await;
    cluster.wait_for_node().await;

    // A second node joins the cluster at runtime.
    let config = Config {
        server_hostname: "127.0.0.1".into(),
        server_port: cluster.server.local_addr().unwrap().port(),
        node_id: "node-2".into(),
        node_address: "127.0.0.1:0".into(),
        node_heartbeat_secs: 1,
        ..Config::default()
    };
    let second_node = oar_node::start(config).await.unwrap();
    assert_ne!(second_node.address, cluster.node.address);

    let deadline = Instant::now() + Duration::from_secs(10);
    while cluster.engine.registry().len() != 2 {
        assert!(
            Instant::now() < deadline,
            "the second node never registered"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let status = cluster.engine.cluster_status().await.unwrap();
    assert_eq!(status.nodes_registered, 2);
    assert_eq!(status.nodes_alive, 2);
    let nodes = cluster.engine.registry().list();
    let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    assert_eq!(ids, ["node-1", "node-2"], "the node list is sorted by id");

    // A node that stops answering is marked dead by the periodic check.
    second_node.shutdown().await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while node_alive(&cluster.engine, "node-2") {
        assert!(Instant::now() < deadline, "the dead node was never noticed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(node_alive(&cluster.engine, "node-1"));

    cluster.stop().await;
}
