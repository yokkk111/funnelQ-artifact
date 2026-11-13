use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use funnelq_residual_obligation::ResidualEpochManifest;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(about = "Build or verify the query-oblivious Residual epoch Manifest")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Build {
        #[arg(long)]
        source_formal_manifest: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Verify {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        source_formal_manifest: PathBuf,
    },
}

fn main() -> Result<()> {
    match Args::parse().command {
        Command::Build {
            source_formal_manifest,
            output,
        } => {
            let manifest = ResidualEpochManifest::build_before_query(&source_formal_manifest)?;
            if let Some(parent) = output.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            manifest.write_canonical(&output)?;
            println!("{}", manifest.epoch_root);
        }
        Command::Verify {
            manifest,
            source_formal_manifest,
        } => {
            let manifest = ResidualEpochManifest::load_canonical(&manifest)?;
            manifest.verify_against_formal_artifact(&source_formal_manifest)?;
            println!("{}", manifest.epoch_root);
        }
    }
    Ok(())
}
