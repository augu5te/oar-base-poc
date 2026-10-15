//! Shared helpers of the OAR base system.
//!
//! * [`types`] — domain types (jobs, states, nodes) used by every component;
//! * [`db`] — the [`db::Database`] trait and its in-memory placeholder;
//! * [`registry`] — the compute node registry;
//! * [`resources`] — parsing of the OAR `-l` resource requests;
//! * [`config`] — OAR3-compatible configuration loading;
//! * [`grpc`] — TCP/unix-socket gRPC connectors;
//! * [`ipc`] — the JSON lines protocol between the CLI and the client daemon;
//! * [`convert`] — domain <-> wire conversions.
//!
//! The generated protocol messages and services are re-exported at the crate
//! root, so `oar_common::NotifyNewJobRequest` works as well as
//! `oar_proto::NotifyNewJobRequest`.

pub use oar_proto::*;

pub mod config;
pub mod convert;
pub mod db;
pub mod grpc;
pub mod ipc;
pub mod registry;
pub mod resources;
pub mod types;
