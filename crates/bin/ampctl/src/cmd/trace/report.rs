use std::path::PathBuf;

use anyhow::bail;

use super::{FilterArgs, RemoteArgs, SERVICE, basic_auth, parse_filters, time::parse_time};

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
        Commands::Query { input, output_dir } => (input, output_dir, super::roots::QUERY, "query"),
        Commands::DerivedDataset { input, output_dir } => (
            input,
            output_dir,
            super::roots::DERIVED_DATASET,
            "derived_dataset",
        ),
        Commands::RawDataset { input, output_dir } => {
            (input, output_dir, super::roots::RAW_DATASET, "raw_dataset")
        }
    };

    let tags = parse_filters(&input.filter.filters)?;
    let after = input.filter.after.as_deref().map(parse_time).transpose()?;
    let before = input.filter.before.as_deref().map(parse_time).transpose()?;
    let auth = basic_auth();

    let full_trace = match &input.file {
        Some(path) => {
            let trace = super::load_trace(path)?;
            eprintln!("Loaded {} spans from {}", trace.spans.len(), path.display());
            trace
        }
        None => {
            let primary_operation = root_spans.first().copied();
            // Search without tag filters — VictoriaTraces returns severely
            // truncated spans when server-side tag filters are active.
            // Tags are applied client-side below.
            let params = super::jaeger::SearchParams {
                service: SERVICE,
                operation: primary_operation,
                limit: input.filter.limit,
                tags: &[],
                start_us: after,
                end_us: before,
            };
            let traces =
                super::jaeger::search_traces(&input.remote.jaeger_url, &params, auth.as_deref())
                    .await?;

            if traces.is_empty() {
                bail!("no traces found matching filters");
            }

            // Client-side tag filtering: keep only traces where at least one
            // span has ALL the requested tag key=value pairs.
            let traces = if tags.is_empty() {
                traces
            } else {
                let filtered: Vec<_> = traces
                    .into_iter()
                    .filter(|trace| {
                        trace.spans.iter().any(|span| {
                            tags.iter().all(|(key, value)| {
                                span.tags.iter().any(|tag| {
                                    tag.key == *key
                                        && tag.value.as_str().is_some_and(|v| v == value)
                                })
                            })
                        })
                    })
                    .collect();
                if filtered.is_empty() {
                    bail!(
                        "no traces matched tag filters: {}",
                        tags.iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                filtered
            };

            let merged = super::merge_traces(traces);
            eprintln!("Fetched {} spans from Jaeger", merged.spans.len());
            merged
        }
    };

    let trace = full_trace.filter_to_subtrees(root_spans);
    eprintln!(
        "Filtered to {} spans (roots: {:?})",
        trace.spans.len(),
        root_spans,
    );

    super::generate_report(&trace, output_dir, prefix)?;

    Ok(())
}
