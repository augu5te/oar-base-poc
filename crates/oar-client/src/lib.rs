//! The OAR client daemon.
//!
//! One `oar-client` process runs per client machine and is the only thing the
//! user commands (`oarsub`, `oarstat`) talk to, over a unix domain socket with
//! one JSON document per line. It stores submissions in the (placeholder)
//! database and notifies the unique server over gRPC.
//!
//! ```text
//!   oarsub/oarstat ── JSON/UDS ──> oar-client ── gRPC ──> oar-server
//!                                     │
//!                                     └── Database (placeholder)
//! ```

pub mod backend;
pub mod daemon;
pub mod runtime;

pub use backend::{ClientError, GrpcServerClient, ServerClient};
pub use daemon::{
    ClientDaemon, bind_socket, handle_connection, serve, serve_listener, serve_with_shutdown,
};
pub use runtime::{RunningClient, start};
