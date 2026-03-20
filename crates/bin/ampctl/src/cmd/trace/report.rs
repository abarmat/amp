use std::path::PathBuf;

use trace_report::time::parse_time;

use super::{FilterArgs, RemoteArgs, SERVICE, basic_auth, parse_filters};

/// Input source: Jaeger search or local file.
#[derive(Debug, clap::Args, Clone)]
pub struct InputArgs {
    #[command(flatten)]
    remote: RemoteArgs,
    #[command(flatten)]
    filter: FilterArgs,

    /// Read from a local trace JSON file instead of searching Jaeger.
    #[arg(long, conflicts_with_all = ["after", "before", "filters"])]
    file: Option<PathBuf>,
}

#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Query execution (Flight SQL or JSONL)
    Query {
        #[command(flatten)]
        input: InputArgs,
        /// Output directory
        #[arg(short, long, default_value = ".")]
        output_dir: PathBuf,
    },
    /// Derived dataset materialization
    DerivedDataset {
        #[command(flatten)]
        input: InputArgs,
        /// Output directory
        #[arg(short, long, default_value = ".")]
        output_dir: PathBuf,
    },
    /// Raw dataset extraction
    RawDataset {
        #[command(flatten)]
        input: InputArgs,
        /// Output directory
        #[arg(short, long, default_value = ".")]
        output_dir: PathBuf,
    },
}

pub async fn run(command: Commands) -> anyhow::Result<()> {
    let (input, output_dir, root_spans, prefix) = match &command {
        Commands::Query { input, output_dir } => {
            (input, output_dir, trace_report::roots::QUERY, "query")
        }
        Commands::DerivedDataset { input, output_dir } => (
            input,
            output_dir,
            trace_report::roots::DERIVED_DATASET,
            "derived_dataset",
        ),
        Commands::RawDataset { input, output_dir } => (
            input,
            output_dir,
            trace_report::roots::RAW_DATASET,
            "raw_dataset",
        ),
    };

    let tags = parse_filters(&input.filter.filters)?;
    let after = input.filter.after.as_deref().map(parse_time).transpose()?;
    let before = input.filter.before.as_deref().map(parse_time).transpose()?;
    let auth = basic_auth();

    let trace = trace_report::fetch_and_filter(
        &input.remote.jaeger_url,
        auth.as_deref(),
        &tags,
        after,
        before,
        input.filter.limit,
        SERVICE,
        root_spans,
        input.file.as_deref(),
    )
    .await?;

    trace_report::generate_report(&trace, output_dir, prefix)?;

    Ok(())
}
