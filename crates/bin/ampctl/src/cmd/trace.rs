mod fetch;
mod flamegraph;
mod jaeger;
mod report;
mod search;
mod time;

use std::path::Path;

use anyhow::{Context, Result};
use flamegraph::FlamegraphConfig;

const SERVICE: &str = "tracing";

/// Root span names for each report type.
pub mod roots {
    pub const QUERY: &[&str] = &["do_get"];
    pub const DERIVED_DATASET: &[&str] = &[
        "execute_microbatch",
        "next_microbatch_range",
        "write",
        "close",
        "register",
        "send_location_change_notif",
    ];
    pub const RAW_DATASET: &[&str] = &["run_range"];
}

#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Generate a performance report (flamegraphs, folded stacks, span tree)
    #[command(subcommand)]
    Report(report::Commands),
    /// Search for recent traces by operation name and filters
    Search(search::Args),
    /// Fetch a trace from Jaeger by ID and save as JSON
    Fetch(fetch::Args),
}

pub async fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::Report(command) => report::run(command).await?,
        Commands::Search(args) => search::run(args).await?,
        Commands::Fetch(args) => fetch::run(args).await?,
    }
    Ok(())
}

/// Shared args for connecting to a Jaeger-compatible API.
#[derive(Debug, clap::Args, Clone)]
pub struct RemoteArgs {
    /// Jaeger-compatible API base URL.
    /// For VictoriaTraces, include the prefix: https://host/select/jaeger
    #[arg(long, env = "JAEGER_URL", default_value = "http://localhost:16686")]
    pub jaeger_url: String,
}

/// Shared args for filtering spans from Jaeger.
#[derive(Debug, clap::Args, Clone)]
pub struct FilterArgs {
    /// Tag filter as key=value. Can be repeated.
    /// Example: --filter job_id=2 --filter dataset=_/block_gas
    #[arg(long = "filter", value_name = "KEY=VALUE")]
    pub filters: Vec<String>,

    /// Only include spans starting after this time.
    /// Accepts ISO 8601 (2026-03-13T17:00:00Z) or relative duration (5m, 1h, 2d — relative to now).
    #[arg(long)]
    pub after: Option<String>,

    /// Only include spans starting before this time.
    /// Accepts ISO 8601 (2026-03-13T17:00:00Z) or relative duration (5m, 1h, 2d — relative to now).
    #[arg(long)]
    pub before: Option<String>,

    /// Maximum number of traces to fetch from Jaeger
    #[arg(long, default_value = "20")]
    pub limit: u32,
}

/// Auth comes from JAEGER_AUTH env var (user:password).
pub fn basic_auth() -> Option<String> {
    std::env::var("JAEGER_AUTH").ok()
}

pub fn parse_filters(filters: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    filters
        .iter()
        .map(|f| {
            let (k, v) = f
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid filter {f:?}, expected key=value"))?;
            Ok((k.to_string(), v.to_string()))
        })
        .collect()
}

/// Generate a full report: trace JSON, wallclock SVG, busy SVG, folded stacks.
fn generate_report(trace: &jaeger::Trace, output_dir: &Path, prefix: &str) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating {}", output_dir.display()))?;

    // Save trace
    let json_path = output_dir.join(format!("{prefix}_trace.json.gz"));
    save_trace_gz(trace, &json_path)?;
    eprintln!("Wrote trace to {}", json_path.display());

    // Wallclock flamegraph
    let wallclock_config = FlamegraphConfig {
        min_duration_us: 0,
        use_busy_time: false,
    };
    let wallclock_path = output_dir.join(format!("{prefix}_wallclock.svg"));
    flamegraph::generate(trace, &wallclock_path, &wallclock_config)?;
    eprintln!("Wrote wallclock flamegraph to {}", wallclock_path.display());

    // Busy-time flamegraph
    let busy_config = FlamegraphConfig {
        min_duration_us: 0,
        use_busy_time: true,
    };
    let busy_path = output_dir.join(format!("{prefix}_busy.svg"));
    flamegraph::generate(trace, &busy_path, &busy_config)?;
    eprintln!("Wrote busy-time flamegraph to {}", busy_path.display());

    // Folded stacks
    let folded_path = output_dir.join(format!("{prefix}_folded.txt"));
    let folded = flamegraph::build_folded(trace, &wallclock_config)?;
    let folded_text = folded.join("\n");
    std::fs::write(&folded_path, &folded_text)?;
    eprintln!("Wrote folded stacks to {}", folded_path.display());

    Ok(())
}

/// Load a trace from a JSON or JSON.gz file.
fn load_trace(path: &Path) -> Result<jaeger::Trace> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if path.to_string_lossy().ends_with(".gz") {
        use std::io::Read;

        use flate2::read::GzDecoder;
        let mut decoder = GzDecoder::new(&bytes[..]);
        let mut json = String::new();
        decoder.read_to_string(&mut json)?;
        Ok(serde_json::from_str(&json)?)
    } else {
        Ok(serde_json::from_slice(&bytes)?)
    }
}

/// Save a trace as gzipped JSON.
fn save_trace_gz(trace: &jaeger::Trace, path: &Path) -> Result<()> {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};
    let json = serde_json::to_vec(trace)?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&json)?;
    std::fs::write(path, encoder.finish()?)?;
    Ok(())
}

/// Merge multiple Jaeger traces into one.
fn merge_traces(traces: Vec<jaeger::Trace>) -> jaeger::Trace {
    let mut iter = traces.into_iter();
    let mut merged = iter.next().expect("at least one trace");
    for trace in iter {
        merged.spans.extend(trace.spans);
        merged.processes.extend(trace.processes);
    }
    merged
}
