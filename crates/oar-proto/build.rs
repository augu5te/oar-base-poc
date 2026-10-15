//! Build script: compiles `proto/oar.proto` into Rust with tonic/prost.
//!
//! `protoc` is required. A user-provided `PROTOC` environment variable always
//! wins; otherwise we look for `protoc` on `PATH` and forward it to prost.

use std::path::PathBuf;

fn find_on_path(binary: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(binary))
        .find(|candidate| candidate.is_file())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/oar.proto");
    println!("cargo:rerun-if-env-changed=PROTOC");

    if std::env::var_os("PROTOC").is_none() {
        match find_on_path("protoc") {
            // SAFETY: build scripts are single threaded, no other thread reads
            // the environment while we mutate it.
            Some(protoc) => unsafe { std::env::set_var("PROTOC", protoc) },
            None => return Err("protoc not found: install protobuf-compiler or set $PROTOC".into()),
        }
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/oar.proto"], &["proto"])?;

    Ok(())
}
