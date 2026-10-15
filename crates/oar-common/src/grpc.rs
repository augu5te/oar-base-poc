//! gRPC transport helpers: address normalization, TCP and unix-domain-socket
//! connectors, and UDS listener plumbing.
//!
//! The inter-daemon protocol runs over plain TCP (`host:port`), while the same
//! services can be exposed over a unix domain socket by using a `unix:` address,
//! which tonic understands natively.

use std::path::{Path, PathBuf};

use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    #[error("invalid address `{address}`: {source}")]
    InvalidAddress {
        address: String,
        source: tonic::transport::Error,
    },
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("i/o error on {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// Turns `host:port` into `http://host:port`; complete URIs (`http://`, `https://`,
/// `unix:`) are returned unchanged.
pub fn normalize_address(address: &str) -> String {
    let address = address.trim();
    if address.starts_with("http://")
        || address.starts_with("https://")
        || address.starts_with("unix:")
    {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

/// Builds a tonic [`Endpoint`] from a `host:port`, URI or `unix:` address.
pub fn endpoint(address: &str) -> Result<Endpoint, GrpcError> {
    let normalized = normalize_address(address);
    Endpoint::from_shared(normalized.clone()).map_err(|source| GrpcError::InvalidAddress {
        address: normalized,
        source,
    })
}

/// Connects eagerly to `address`.
pub async fn connect(address: &str) -> Result<Channel, GrpcError> {
    Ok(endpoint(address)?.connect().await?)
}

/// Builds a [`Channel`] that connects on first use.
pub fn connect_lazy(address: &str) -> Result<Channel, GrpcError> {
    Ok(endpoint(address)?.connect_lazy())
}

/// `unix:` URI for a socket path.
pub fn unix_address(path: &Path) -> String {
    format!("unix://{}", path.display())
}

/// Connects to a gRPC service listening on a unix domain socket.
pub async fn connect_unix(path: &Path) -> Result<Channel, GrpcError> {
    connect(&unix_address(path)).await
}

/// Binds a unix domain socket, removing a stale socket file first.
pub fn bind_unix(path: &Path) -> Result<UnixListener, GrpcError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|source| GrpcError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    if path.exists() {
        std::fs::remove_file(path).map_err(|source| GrpcError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    UnixListener::bind(path).map_err(|source| GrpcError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Wraps a UDS listener into a stream usable with `Server::serve_with_incoming`.
pub fn unix_incoming(listener: UnixListener) -> UnixListenerStream {
    UnixListenerStream::new(listener)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    #[test]
    fn addresses_are_normalized() {
        assert_eq!(normalize_address("127.0.0.1:6666"), "http://127.0.0.1:6666");
        assert_eq!(normalize_address("localhost:1"), "http://localhost:1");
        assert_eq!(normalize_address("http://h:1"), "http://h:1");
        assert_eq!(
            normalize_address("unix:///tmp/x.sock"),
            "unix:///tmp/x.sock"
        );
        assert_eq!(normalize_address("  h:2  "), "http://h:2");
    }

    #[test]
    fn invalid_addresses_are_rejected() {
        assert!(matches!(
            endpoint("http://"),
            Err(GrpcError::InvalidAddress { .. })
        ));
    }

    #[test]
    fn unix_address_format() {
        assert_eq!(
            unix_address(Path::new("/run/oar/server.sock")),
            "unix:///run/oar/server.sock"
        );
    }

    #[tokio::test]
    async fn bind_unix_removes_stale_socket_and_accepts_connections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/oar.sock");

        // Create a stale regular file where the socket should go.
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"stale").unwrap();

        let listener = bind_unix(&path).unwrap();
        assert!(path.exists());

        let client = tokio::spawn({
            let path = path.clone();
            async move { tokio::net::UnixStream::connect(path).await.unwrap() }
        });

        let mut incoming = unix_incoming(listener);
        let accepted = incoming.next().await.unwrap().unwrap();
        let client_stream = client.await.unwrap();
        assert!(accepted.peer_addr().is_ok());
        assert!(client_stream.peer_addr().is_ok());
    }
}
