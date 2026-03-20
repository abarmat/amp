use std::path::PathBuf;

use super::{RemoteArgs, basic_auth, jaeger};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    remote: RemoteArgs,
    /// Trace ID to fetch
    #[arg(long)]
    trace_id: String,
    /// Output JSON file (.json or .json.gz)
    #[arg(short, long)]
    output: PathBuf,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let auth = basic_auth();
    let trace =
        jaeger::fetch_trace(&args.remote.jaeger_url, &args.trace_id, auth.as_deref()).await?;

    if args.output.to_string_lossy().ends_with(".gz") {
        super::save_trace_gz(&trace, &args.output)?;
    } else {
        let json = serde_json::to_vec(&trace)?;
        std::fs::write(&args.output, json)?;
    }

    eprintln!(
        "Wrote trace ({} spans) to {}",
        trace.spans.len(),
        args.output.display()
    );
    Ok(())
}
