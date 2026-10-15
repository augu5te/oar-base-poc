//! `oarstat`: show the state of the OAR jobs.

use std::process::ExitCode;

use clap::Parser;
use oar_cli::OarstatArgs;

#[tokio::main]
async fn main() -> ExitCode {
    let args = OarstatArgs::parse();
    match oar_cli::stat_command(args).await {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("oarstat: {error}");
            ExitCode::FAILURE
        }
    }
}
