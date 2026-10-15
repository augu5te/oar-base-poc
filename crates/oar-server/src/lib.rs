//! The unique OAR server daemon.
//!
//! ```text
//!            gRPC (SchedulerService, NodeCallbackService)
//!   clients ────────────────────────────────┐
//!                                           v
//!   nodes ── NodeCallbackService ──> ┌───────────────┐
//!                                    │   automaton   │
//!                                    └───────┬───────┘
//!                     Scheduling/Launching   │   Checking/Finishing
//!                       (worker tasks)       │     (spawned tasks)
//!                                            v
//!                                    ┌───────────────┐
//!                                    │    engine     │  Database (placeholder)
//!                                    └───────┬───────┘  NodeRegistry
//!                                            │
//!                  ComputeNodeService ───────┘  (server -> nodes)
//! ```
//!
//! [`start`] / [`start_unix`] wire everything together; `main` only handles
//! configuration, logging and signals.

pub mod automaton;
pub mod engine;
pub mod error;
pub mod node_client;
pub mod scheduler;
pub mod server;
pub mod services;

pub use automaton::{Automaton, Event, Step, StepExecutor};
pub use engine::{ClusterStatus, Engine, EngineBuilder, EngineSteps, WALLTIME_EXIT_CODE};
pub use error::ServerError;
pub use node_client::{GrpcNodeLauncher, LauncherError, NodeLauncher, NodePing};
pub use scheduler::{Assignment, FifoScheduler, Scheduler};
pub use server::{RunningServer, start, start_unix};
