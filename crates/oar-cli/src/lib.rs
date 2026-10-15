//! The OAR user commands.
//!
//! Both binaries are thin wrappers around this library: they parse their
//! arguments, build the JSON request of [`oar_common::ipc`] and send it to the
//! client daemon over its unix domain socket.

use std::path::{Path, PathBuf};

use clap::Parser;
use oar_common::config::Config;
use oar_common::ipc::{self, IpcError, IpcRequest, IpcResponse, JobSummary};
use oar_common::resources::{ResourceRequest, parse_resource_list};
use oar_common::types::{JobId, JobState};

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(transparent)]
    Ipc(#[from] IpcError),
    #[error(transparent)]
    Resource(#[from] oar_common::resources::ResourceError),
    #[error(transparent)]
    Config(#[from] oar_common::config::ConfigError),
    #[error("{0}")]
    Invalid(String),
    #[error("the client daemon refused the request: {0}")]
    Daemon(String),
}

impl CliError {
    /// Ready-to-print hint when the client daemon is not reachable.
    pub fn with_hint(self, socket: &Path) -> String {
        if let CliError::Ipc(IpcError::Io(error)) = &self
            && matches!(
                error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
            )
        {
            return format!(
                "{error}: is the OAR client daemon running on {}?",
                socket.display()
            );
        }
        self.to_string()
    }
}

/// `oarsub` command line.
#[derive(Debug, Clone, Parser)]
#[command(name = "oarsub", about = "Submit a job to the OAR cluster")]
pub struct OarsubArgs {
    /// Resource request in OAR `-l` syntax, e.g. `nodes=2,walltime=1:00:00`.
    /// May be repeated; the parts are joined with `/`.
    #[arg(short = 'l', long = "resource", value_name = "RESOURCES")]
    pub resources: Vec<String>,

    /// Walltime, as an alternative to `-l walltime=...`.
    #[arg(long = "walltime", value_name = "WALLTIME")]
    pub walltime: Option<String>,

    /// Path of the client daemon socket (defaults to `$OAR_CLIENT_SOCKET`).
    #[arg(long = "socket", value_name = "PATH")]
    pub socket: Option<PathBuf>,

    /// Command to run on the allocated nodes.
    #[arg(
        value_name = "COMMAND",
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    pub command: Vec<String>,
}

/// `oarstat` command line.
#[derive(Debug, Clone, Parser)]
#[command(name = "oarstat", about = "Show the state of OAR jobs")]
pub struct OarstatArgs {
    /// Show a single job instead of the whole list.
    #[arg(short = 'j', long = "job", value_name = "JOB_ID")]
    pub job_id: Option<JobId>,

    /// Only list the jobs in this state (e.g. `waiting`, `running`).
    #[arg(short = 's', long = "state", value_name = "STATE")]
    pub state: Option<String>,

    /// Path of the client daemon socket (defaults to `$OAR_CLIENT_SOCKET`).
    #[arg(long = "socket", value_name = "PATH")]
    pub socket: Option<PathBuf>,
}

/// A submission after argument parsing and resource decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub command: String,
    pub nodes: u32,
    pub walltime_secs: u32,
    pub resources: ResourceRequest,
}

/// Joins the `-l` options (and the `--walltime` shortcut) into a single OAR
/// resource list.
pub fn resource_list(
    resources: &[String],
    walltime: Option<&str>,
    default_resources: &str,
) -> String {
    let mut parts: Vec<String> = resources
        .iter()
        .map(|part| part.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    if let Some(walltime) = walltime
        && !walltime.trim().is_empty()
        && !parts
            .iter()
            .any(|part| part.to_ascii_lowercase().contains("walltime"))
    {
        parts.push(format!("walltime={}", walltime.trim()));
    }
    if parts.is_empty() {
        return default_resources.to_string();
    }
    parts.join("/")
}

/// Turns parsed arguments into a validated [`Submission`].
pub fn plan_submission(config: &Config, args: &OarsubArgs) -> Result<Submission, CliError> {
    let command = args.command.join(" ");
    if command.trim().is_empty() {
        return Err(CliError::Invalid(
            "no command given: usage: oarsub [-l RESOURCES] COMMAND".into(),
        ));
    }
    let resources = parse_resource_list(&resource_list(
        &args.resources,
        args.walltime.as_deref(),
        &config.default_resources,
    ))?;
    Ok(Submission {
        command,
        nodes: resources.nodes.max(1),
        walltime_secs: resources.walltime_secs,
        resources,
    })
}

/// Resolves the daemon socket: explicit `--socket`, else the configuration.
pub fn socket_path(config: &Config, explicit: Option<&Path>) -> PathBuf {
    explicit
        .map(Path::to_path_buf)
        .unwrap_or_else(|| config.socket_path())
}

/// Sends one IPC request and returns the response.
pub async fn request(socket: &Path, request: &IpcRequest) -> Result<IpcResponse, CliError> {
    Ok(ipc::request(socket, request).await?)
}

/// Runs `oarsub`, returning the text to print.
pub async fn submit_command(args: OarsubArgs) -> Result<String, CliError> {
    let config = Config::load(None)?;
    let socket = socket_path(&config, args.socket.as_deref());
    let submission = plan_submission(&config, &args)?;
    submit(&socket, &submission).await
}

/// Sends one submission and formats the result.
pub async fn submit(socket: &Path, submission: &Submission) -> Result<String, CliError> {
    let message = IpcRequest::Submit {
        command: submission.command.clone(),
        nodes: submission.nodes,
        walltime_secs: submission.walltime_secs,
    };
    let response = match request(socket, &message).await {
        Ok(response) => response,
        Err(error) => return Err(CliError::Invalid(error.with_hint(socket))),
    };
    match response {
        IpcResponse::Submitted { job_id, warning } => {
            let mut text = format!("Job {job_id} submitted");
            if let Some(warning) = warning {
                text.push_str(&format!(" (warning: {warning})"));
            }
            Ok(text)
        }
        IpcResponse::Error { message } => Err(CliError::Daemon(message)),
        other => Err(CliError::Invalid(format!(
            "unexpected daemon response: {other:?}"
        ))),
    }
}

/// Runs `oarstat`, returning the text to print.
pub async fn stat_command(args: OarstatArgs) -> Result<String, CliError> {
    let config = Config::load(None)?;
    let socket = socket_path(&config, args.socket.as_deref());
    let state = match args.state.as_deref() {
        Some(state) if !state.trim().is_empty() => Some(
            JobState::parse(state)
                .ok_or_else(|| CliError::Invalid(format!("unknown job state `{state}`")))?,
        ),
        _ => None,
    };
    stat(&socket, args.job_id, state).await
}

/// Queries the daemon for one job or for the job list.
pub async fn stat(
    socket: &Path,
    job_id: Option<JobId>,
    state: Option<JobState>,
) -> Result<String, CliError> {
    let message = job_id.map_or(IpcRequest::List { state }, |job_id| IpcRequest::Status {
        job_id,
    });
    let response = match request(socket, &message).await {
        Ok(response) => response,
        Err(error) => return Err(CliError::Invalid(error.with_hint(socket))),
    };
    match response {
        IpcResponse::Status { job } => Ok(format_job(&job)),
        IpcResponse::Jobs { jobs } => Ok(format_jobs(&jobs)),
        IpcResponse::Error { message } => Err(CliError::Daemon(message)),
        other => Err(CliError::Invalid(format!(
            "unexpected daemon response: {other:?}"
        ))),
    }
}

/// Header of the `oarstat` table.
pub const JOB_TABLE_HEADER: &str = "JOB_ID  STATE       NODES                 EXIT";

/// Formats a single job.
pub fn format_job(job: &JobSummary) -> String {
    format!(
        "{:<7} {:<11} {:<21} {}",
        job.job_id,
        job.state,
        nodes_column(job),
        exit_column(job)
    )
}

/// Formats the whole job table.
pub fn format_jobs(jobs: &[JobSummary]) -> String {
    let mut lines = vec![JOB_TABLE_HEADER.to_string()];
    lines.extend(jobs.iter().map(format_job));
    if jobs.is_empty() {
        lines.push("(no job)".to_string());
    }
    lines.join("\n")
}

fn nodes_column(job: &JobSummary) -> String {
    if job.nodes.is_empty() {
        "-".to_string()
    } else {
        job.nodes.join(",")
    }
}

fn exit_column(job: &JobSummary) -> String {
    job.exit_code
        .map_or_else(|| "-".to_string(), |code| code.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(resources: &[&str], walltime: Option<&str>, command: &[&str]) -> OarsubArgs {
        OarsubArgs {
            resources: resources.iter().map(|r| r.to_string()).collect(),
            walltime: walltime.map(str::to_string),
            socket: None,
            command: command.iter().map(|c| c.to_string()).collect(),
        }
    }

    #[test]
    fn command_line_parsing() {
        let parsed = OarsubArgs::try_parse_from([
            "oarsub",
            "-l",
            "nodes=2",
            "-l",
            "walltime=1:00:00",
            "sleep",
            "10",
        ])
        .unwrap();
        assert_eq!(parsed.resources, vec!["nodes=2", "walltime=1:00:00"]);
        assert_eq!(parsed.command, vec!["sleep", "10"]);

        let parsed = OarstatArgs::try_parse_from(["oarstat", "-j", "42", "-s", "running"]).unwrap();
        assert_eq!(parsed.job_id, Some(42));
        assert_eq!(parsed.state.as_deref(), Some("running"));

        assert!(OarstatArgs::try_parse_from(["oarstat", "-j", "notanumber"]).is_err());
    }

    #[test]
    fn resource_list_joining_and_defaults() {
        assert_eq!(
            resource_list(&["nodes=2".into(), "core=4".into()], None, "/resource_id=1"),
            "nodes=2/core=4"
        );
        assert_eq!(resource_list(&[], None, "/resource_id=1"), "/resource_id=1");
        assert_eq!(
            resource_list(&["nodes=2".into()], Some("1:00:00"), "/resource_id=1"),
            "nodes=2/walltime=1:00:00"
        );
        // The `-l walltime=` already given wins over `--walltime`.
        assert_eq!(
            resource_list(&["nodes=2/walltime=60".into()], Some("999"), "x"),
            "nodes=2/walltime=60"
        );
    }

    #[test]
    fn planning_applies_defaults_and_rejects_empty_commands() {
        let config = Config::default();
        let submission = plan_submission(
            &config,
            &args(&["nodes=3", "walltime=2:00"], None, &["echo", "hi"]),
        )
        .unwrap();
        assert_eq!(submission.command, "echo hi");
        assert_eq!(submission.nodes, 3);
        assert_eq!(submission.walltime_secs, 7200);

        let submission = plan_submission(&config, &args(&[], None, &["echo", "hi"])).unwrap();
        assert_eq!(
            submission.nodes, 1,
            "the default /resource_id=1 means one node"
        );

        assert!(plan_submission(&config, &args(&[], None, &[])).is_err());
        assert!(plan_submission(&config, &args(&["nodes"], None, &["x"])).is_err());
        assert!(plan_submission(&config, &args(&["nodes="], None, &["x"])).is_err());
    }

    #[test]
    fn socket_resolution() {
        let config = Config::default();
        assert_eq!(socket_path(&config, None), config.socket_path());
        assert_eq!(
            socket_path(&config, Some(Path::new("/tmp/x.sock"))),
            PathBuf::from("/tmp/x.sock")
        );
    }

    #[test]
    fn formatting() {
        let job = JobSummary {
            job_id: 7,
            state: JobState::Running,
            nodes: vec!["n1".into(), "n2".into()],
            exit_code: None,
        };
        assert!(format_job(&job).starts_with("7       Running"));
        let text = format_jobs(std::slice::from_ref(&job));
        assert!(text.starts_with(JOB_TABLE_HEADER));
        assert!(text.contains("n1,n2"));

        let done = JobSummary {
            exit_code: Some(0),
            ..job
        };
        assert!(format_job(&done).ends_with('0'));
        assert!(format_jobs(&[]).contains("(no job)"));
    }
}
