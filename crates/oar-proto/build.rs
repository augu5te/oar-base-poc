fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a user-provided protoc if any, otherwise raise an error
    if std::env::var_os("PROTOC").is_none() {
        eprintln!("PROTOC is none, needed to find protoc");
        std::process::exit(1);
    }
    println!("cargo:rerun-if-changed=proto/oar.proto");
    tonic_build::configure().compile_protos(&["proto/oar.proto"], &["proto"])?;
    Ok(())
}
