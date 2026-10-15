//! End-to-end test of `oarsub` / `oarstat` against a real client daemon
//! listening on a unix domain socket (only the server backend is faked).

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use clap::Parser;
use oar_cli::{OarstatArgs, OarsubArgs};
use oar_client::{ClientDaemon, ClientError, ServerClient, start};
use oar_common::config::Config;
use oar_common::db::MemoryDatabase;
use oar_common::ipc::JobSummary;
use oar_common::types::{JobId, JobRequest, JobState};

#[derive(Default)]
struct FakeServer {
    jobs: Mutex<Vec<JobSummary>>,
}

#[async_trait]
impl ServerClient for FakeServer {
    async fn notify_new_job(&self, request: &JobRequest) -> Result<JobId, ClientError> {
        let mut jobs = self.jobs.lock().unwrap();
        let job_id = jobs.len() as JobId + 1;
        jobs.push(JobSummary {
            job_id,
            state: JobState::Waiting,
            nodes: Vec::new(),
            exit_code: None,
        });
        assert!(!request.command.is_empty());
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

struct Cluster {
    _directory: tempfile::TempDir,
    socket: std::path::PathBuf,
    client: oar_client::RunningClient,
    server: Arc<FakeServer>,
}

impl Cluster {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("client.sock");
        let server = Arc::new(FakeServer::default());
        let daemon = Arc::new(ClientDaemon::new(
            Arc::new(MemoryDatabase::new()),
            server.clone(),
            Arc::new(Config::default()),
        ));
        let client = start(daemon, &socket).await.unwrap();
        Self {
            _directory: directory,
            socket,
            client,
            server,
        }
    }

    fn oarsub(&self, extra: &[&str]) -> OarsubArgs {
        let mut argv = vec!["oarsub", "--socket", self.socket.to_str().unwrap()];
        argv.extend_from_slice(extra);
        OarsubArgs::parse_from(argv)
    }

    fn oarstat(&self, extra: &[&str]) -> OarstatArgs {
        let mut argv = vec!["oarstat", "--socket", self.socket.to_str().unwrap()];
        argv.extend_from_slice(extra);
        OarstatArgs::parse_from(argv)
    }

    async fn stop(self) {
        self.client.shutdown().await;
    }
}

#[tokio::test]
async fn oarsub_then_oarstat_through_the_daemon() {
    let cluster = Cluster::start().await;

    let text = oar_cli::submit_command(cluster.oarsub(&["-l", "nodes=2", "sleep", "10"]))
        .await
        .unwrap();
    assert_eq!(text, "Job 1 submitted");

    // The command was forwarded as a single string.
    assert_eq!(cluster.server.jobs.lock().unwrap().len(), 1);

    let text = oar_cli::submit_command(cluster.oarsub(&["--walltime", "1:00", "exit=3"]))
        .await
        .unwrap();
    assert_eq!(text, "Job 2 submitted");

    // `oarstat` without argument lists everything.
    let text = oar_cli::stat_command(cluster.oarstat(&[])).await.unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], oar_cli::JOB_TABLE_HEADER);
    assert_eq!(lines.len(), 3, "header + two jobs:\n{text}");
    assert!(lines[1].starts_with("1 "));
    assert!(lines[2].starts_with("2 "));

    // `oarstat -j` shows one job.
    let text = oar_cli::stat_command(cluster.oarstat(&["-j", "2"]))
        .await
        .unwrap();
    assert!(
        text.starts_with("2       Waiting"),
        "unexpected output: {text}"
    );

    // `oarstat -s` filters.
    let text = oar_cli::stat_command(cluster.oarstat(&["-s", "running"]))
        .await
        .unwrap();
    assert!(text.contains("(no job)"), "unexpected output: {text}");

    // Unknown jobs and states are reported.
    assert!(
        oar_cli::stat_command(cluster.oarstat(&["-j", "42"]))
            .await
            .is_err()
    );
    assert!(
        oar_cli::stat_command(cluster.oarstat(&["-s", "flying"]))
            .await
            .is_err()
    );

    cluster.stop().await;
}

#[tokio::test]
async fn argument_errors_do_not_reach_the_daemon() {
    let cluster = Cluster::start().await;

    let error = oar_cli::submit_command(cluster.oarsub(&[]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no command given"));
    let error = oar_cli::submit_command(cluster.oarsub(&["-l", "nodes", "ls"]))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("malformed"),
        "unexpected error: {error}"
    );

    assert!(cluster.server.jobs.lock().unwrap().is_empty());
    cluster.stop().await;
}

#[tokio::test]
async fn a_missing_daemon_is_explained() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("nobody.sock");

    let args =
        OarsubArgs::parse_from(["oarsub", "--socket", socket.to_str().unwrap(), "echo", "hi"]);
    let error = oar_cli::submit_command(args).await.unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("client daemon"),
        "unexpected error: {message}"
    );

    let args = OarstatArgs::parse_from(["oarstat", "--socket", socket.to_str().unwrap()]);
    assert!(oar_cli::stat_command(args).await.is_err());
}
