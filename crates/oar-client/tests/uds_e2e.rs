//! End-to-end test of the CLI <-> client-daemon protocol over a real unix
//! domain socket: the daemon is started for real (as the `oar-client` binary
//! would), only the server backend is faked.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use oar_client::{ClientDaemon, ClientError, ServerClient, start};
use oar_common::config::Config;
use oar_common::db::MemoryDatabase;
use oar_common::ipc::{self, IpcRequest, IpcResponse, JobSummary};
use oar_common::types::{JobId, JobRequest, JobState};

#[derive(Default)]
struct FakeServer {
    next_id: Mutex<JobId>,
    jobs: Mutex<Vec<JobSummary>>,
}

impl FakeServer {
    fn new() -> Self {
        Self {
            next_id: Mutex::new(1),
            jobs: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ServerClient for FakeServer {
    async fn notify_new_job(&self, request: &JobRequest) -> Result<JobId, ClientError> {
        assert!(
            request.validate().is_ok(),
            "the daemon must validate before notifying"
        );
        let mut next = self.next_id.lock().unwrap();
        let job_id = *next;
        *next += 1;
        self.jobs.lock().unwrap().push(JobSummary {
            job_id,
            state: JobState::Running,
            nodes: vec!["n1".into()],
            exit_code: None,
        });
        Ok(job_id)
    }

    async fn get_job(&self, job_id: JobId) -> Result<Option<JobSummary>, ClientError> {
        Ok(self
            .jobs
            .lock()
            .unwrap()
            .iter()
            .find(|job| job.job_id == job_id)
            .cloned())
    }

    async fn list_jobs(&self, state: Option<JobState>) -> Result<Vec<JobSummary>, ClientError> {
        Ok(self
            .jobs
            .lock()
            .unwrap()
            .iter()
            .filter(|job| state.is_none_or(|state| job.state == state))
            .cloned()
            .collect())
    }
}

#[tokio::test]
async fn ipc_round_trips_over_the_unix_socket() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("oar/client.sock");

    let daemon = Arc::new(ClientDaemon::new(
        Arc::new(MemoryDatabase::new()),
        Arc::new(FakeServer::new()),
        Arc::new(Config::default()),
    ));
    let client = start(daemon, &socket).await.unwrap();
    assert_eq!(client.socket(), socket);

    // Submit.
    let response = ipc::request(
        &socket,
        &IpcRequest::Submit {
            command: "echo hi".into(),
            nodes: 2,
            walltime_secs: 60,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        response,
        IpcResponse::Submitted {
            job_id: 1,
            warning: None
        }
    );

    // Status.
    let response = ipc::request(&socket, &IpcRequest::Status { job_id: 1 })
        .await
        .unwrap();
    match response {
        IpcResponse::Status { job } => {
            assert_eq!(job.job_id, 1);
            assert_eq!(job.state, JobState::Running);
            assert_eq!(job.nodes, ["n1"]);
        }
        other => panic!("unexpected response: {other:?}"),
    }

    // List, filtered and unfiltered.
    let all = ipc::request(&socket, &IpcRequest::List { state: None })
        .await
        .unwrap();
    assert!(matches!(all, IpcResponse::Jobs { jobs } if jobs.len() == 1));
    let none = ipc::request(
        &socket,
        &IpcRequest::List {
            state: Some(JobState::Waiting),
        },
    )
    .await
    .unwrap();
    assert!(matches!(none, IpcResponse::Jobs { jobs } if jobs.is_empty()));

    // Errors are reported, not dropped.
    let response = ipc::request(&socket, &IpcRequest::Status { job_id: 404 })
        .await
        .unwrap();
    assert!(matches!(response, IpcResponse::Error { .. }));

    client.shutdown().await;
}

#[tokio::test]
async fn malformed_and_multiple_messages_on_one_connection() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("client.sock");
    let daemon = Arc::new(ClientDaemon::new(
        Arc::new(MemoryDatabase::new()),
        Arc::new(FakeServer::new()),
        Arc::new(Config::default()),
    ));
    let client = start(daemon, &socket).await.unwrap();

    let stream = tokio::net::UnixStream::connect(&socket).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Garbage, then a valid request, on the same connection.
    writer.write_all(b"{not json}\n").await.unwrap();
    writer
        .write_all(
            b"{\"type\":\"submit\",\"command\":\"echo hi\",\"nodes\":1,\"walltime_secs\":60}\n",
        )
        .await
        .unwrap();
    writer.flush().await.unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let response: IpcResponse = serde_json::from_str(line.trim()).unwrap();
    assert!(matches!(response, IpcResponse::Error { message } if message.contains("malformed")));

    line.clear();
    reader.read_line(&mut line).await.unwrap();
    let response: IpcResponse = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        response,
        IpcResponse::Submitted {
            job_id: 1,
            warning: None
        }
    );

    // Closing the connection must not disturb the daemon.
    drop(writer);
    drop(reader);

    let response = ipc::request(&socket, &IpcRequest::List { state: None })
        .await
        .unwrap();
    assert!(matches!(response, IpcResponse::Jobs { jobs } if jobs.len() == 1));

    client.shutdown().await;
}

#[tokio::test]
async fn the_daemon_binds_even_if_the_socket_file_exists() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("stale.sock");
    std::fs::write(&socket, b"stale").unwrap();

    let daemon = Arc::new(ClientDaemon::new(
        Arc::new(MemoryDatabase::new()),
        Arc::new(FakeServer::new()),
        Arc::new(Config::default()),
    ));
    let client = start(daemon, &socket).await.unwrap();
    // The stale file was replaced by a socket: connecting works.
    assert!(
        ipc::request(&socket, &IpcRequest::List { state: None })
            .await
            .is_ok()
    );
    client.shutdown().await;
}
