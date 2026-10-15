//! Generated wire protocol of the OAR base system.
//!
//! Everything in [`oar`] comes from `proto/oar.proto`; see that file for the
//! four gRPC services and their messages.

#![allow(clippy::all)]

/// Code generated from `proto/oar.proto` (package `oar`).
pub mod oar {
    tonic::include_proto!("oar");
}

pub use oar::*;
