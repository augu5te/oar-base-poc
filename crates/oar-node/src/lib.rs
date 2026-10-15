//! The OAR compute node daemon.
//!
//! One `oar-node` process runs per compute node. It:
//!
//! 1. exposes `ComputeNodeService` (`StartJob`, `Ping`) to the server;
//! 2. registers itself with the server's `NodeCallbackService` and sends
//!    periodic heartbeats (re-registering when a heartbeat is refused);
//! 3. *simulates* the execution of the jobs it is asked to start, and reports
//!    their exit code back to the server.
//!
//! ```text
//!   server ── StartJob/Ping ──> oar-node ── register/heartbeat/JobFinished ──> server
//!                                 │
//!                                 └── FakeExecutor (placeholder)
//! ```

pub mod callback;
pub mod executor;
pub mod runtime;
pub mod service;

pub use callback::{CallbackError, GrpcNodeCallback, NodeCallback};
pub use executor::{Executor, FakeExecutor, JobExecution};
pub use runtime::{RunningNode, start, start_unix, wait_for_shutdown};
pub use service::{ComputeNodeGrpc, NodeError, NodeState, execution_from_request};
