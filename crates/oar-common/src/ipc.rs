//! Protocol between the CLI commands (`oarsub`...) and the client daemon,
//! over a unix domain socket: one JSON document per line.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::types::{JobId, JobState, NodeId};

/// Messages larger than this are refused (the command is capped well below).
pub const MAX_MESSAGE_BYTES: usize = 128 * 1024;
pub const SOCKET_ENV: &str = "OAR_CLIENT_SOCKET";
pub const DEFAULT_SOCKET: &str = "/run/oar/client.sock";

pub fn default_socket_path() -> PathBuf {
    std::env::var_os(SOCKET_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| DEFAULT_SOCKET.into())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IpcRequest {
    /// The submitter's identity is NOT part of the message: the daemon takes it
    /// from the socket credentials.
    Submit {
        command: String,
        nodes: u32,
        walltime_secs: u32,
    },
    Status {
        job_id: JobId,
    },
    /// Lists the jobs known to the client daemon / the server.
    List {
        state: Option<JobState>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSummary {
    pub job_id: JobId,
    pub state: JobState,
    pub nodes: Vec<NodeId>,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IpcResponse {
    Submitted {
        job_id: JobId,
        warning: Option<String>,
    },
    Status {
        job: JobSummary,
    },
    Jobs {
        jobs: Vec<JobSummary>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed message: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("message larger than {MAX_MESSAGE_BYTES} bytes")]
    TooLarge,
    #[error("connection closed before a response was received")]
    Closed,
}

/// Reads one line (without the trailing newline); `None` on clean EOF.
pub async fn read_line_bounded<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<String>, IpcError> {
    let mut limited = reader.take(MAX_MESSAGE_BYTES as u64 + 1);
    let mut line = String::new();
    let n = limited.read_line(&mut line).await?;
    if n == 0 {
        return Ok(None);
    }
    if line.len() > MAX_MESSAGE_BYTES {
        return Err(IpcError::TooLarge);
    }
    Ok(Some(line.trim_end_matches(['\n', '\r']).to_string()))
}

pub async fn write_message<W, T>(writer: &mut W, msg: &T) -> Result<(), IpcError>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let mut bytes = serde_json::to_vec(msg)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Sends one request to the client daemon listening on `socket`.
pub async fn request(socket: &Path, req: &IpcRequest) -> Result<IpcResponse, IpcError> {
    let stream = UnixStream::connect(socket).await?;
    let (rd, mut wr) = stream.into_split();
    write_message(&mut wr, req).await?;
    let mut rd = BufReader::new(rd);
    let line = read_line_bounded(&mut rd).await?.ok_or(IpcError::Closed)?;
    Ok(serde_json::from_str(&line)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_roundtrip_with_stable_wire_format() {
        let r = IpcRequest::Submit {
            command: "ls".into(),
            nodes: 2,
            walltime_secs: 60,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(
            json,
            r#"{"type":"submit","command":"ls","nodes":2,"walltime_secs":60}"#
        );
        assert_eq!(serde_json::from_str::<IpcRequest>(&json).unwrap(), r);
    }

    #[test]
    fn list_request_has_a_stable_wire_format() {
        let r = IpcRequest::List { state: None };
        assert_eq!(
            serde_json::to_string(&r).unwrap(),
            r#"{"type":"list","state":null}"#
        );
        let r = IpcRequest::List {
            state: Some(JobState::Running),
        };
        assert_eq!(
            serde_json::to_string(&r).unwrap(),
            r#"{"type":"list","state":"running"}"#
        );
        assert_eq!(
            serde_json::from_str::<IpcRequest>(r#"{"type":"list","state":null}"#).unwrap(),
            IpcRequest::List { state: None }
        );
    }

    #[test]
    fn responses_roundtrip() {
        let summary = JobSummary {
            job_id: 1,
            state: JobState::Running,
            nodes: vec!["n1".into()],
            exit_code: None,
        };
        let responses = [
            IpcResponse::Submitted {
                job_id: 3,
                warning: Some("w".into()),
            },
            IpcResponse::Error {
                message: "no".into(),
            },
            IpcResponse::Status {
                job: summary.clone(),
            },
            IpcResponse::Jobs {
                jobs: vec![summary],
            },
        ];
        for r in responses {
            let json = serde_json::to_string(&r).unwrap();
            assert_eq!(serde_json::from_str::<IpcResponse>(&json).unwrap(), r);
        }
    }

    #[test]
    fn unknown_request_type_is_rejected() {
        assert!(serde_json::from_str::<IpcRequest>(r#"{"type":"format_disk"}"#).is_err());
    }

    #[tokio::test]
    async fn bounded_line_reader() {
        let mut ok = BufReader::new(&b"hello\nworld\n"[..]);
        assert_eq!(
            read_line_bounded(&mut ok).await.unwrap().as_deref(),
            Some("hello")
        );
        assert_eq!(
            read_line_bounded(&mut ok).await.unwrap().as_deref(),
            Some("world")
        );
        assert_eq!(read_line_bounded(&mut ok).await.unwrap(), None);

        let big = vec![b'a'; MAX_MESSAGE_BYTES + 10];
        let mut reader = BufReader::new(&big[..]);
        assert!(matches!(
            read_line_bounded(&mut reader).await,
            Err(IpcError::TooLarge)
        ));
    }
}
