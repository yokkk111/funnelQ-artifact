use anyhow::Result;
use clap::Parser;
use hgmatch_same_tx_rust_fair::{query_pipeline, query_pipeline_warm};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long)]
    db: String,
    #[arg(long = "query-json")]
    query_json: String,
    #[arg(long)]
    outdir: String,
    #[arg(long, default_value_t = 1)]
    threads: usize,

    #[arg(long = "summary-only", default_value_t = false)]
    summary_only: bool,

    #[arg(long = "warmup-runs", default_value_t = 0)]
    warmup_runs: usize,

    #[arg(long = "phase-marker")]
    phase_marker: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let summary = if args.warmup_runs > 0 || args.phase_marker.is_some() {
        query_pipeline_warm(
            &args.db,
            &args.query_json,
            &args.outdir,
            args.threads,
            args.summary_only,
            args.warmup_runs,
            args.phase_marker.as_deref(),
        )?
    } else {
        query_pipeline(&args.db, &args.query_json, &args.outdir, args.threads, args.summary_only)?
    };
    println!("=== HGMatch Rust Query Summary ===");
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
