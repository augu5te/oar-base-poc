//! `oarsub`: submit a job to the OAR cluster.

use std::process::ExitCode;

use clap::Parser;
use oar_cli::OarsubArgs;

#[tokio::main]
async fn main() -> ExitCode {
    let args = OarsubArgs::parse();
    match oar_cli::submit_command(args).await {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("oarsub: {error}");
            ExitCode::FAILURE
        }
    }
}
