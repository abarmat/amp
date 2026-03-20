mod fetch;
mod report;
mod search;

const SERVICE: &str = "tracing";

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
