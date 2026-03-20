use trace_report::{
    jaeger,
    time::{format_epoch_secs, parse_time},
};

use super::{FilterArgs, RemoteArgs, SERVICE, basic_auth, parse_filters};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    remote: RemoteArgs,
    #[command(flatten)]
    filter: FilterArgs,
    /// Operation name to search for
    #[arg(long)]
    operation: Option<String>,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let tags = parse_filters(&args.filter.filters)?;
    let start_us = args.filter.after.as_deref().map(parse_time).transpose()?;
    let end_us = args.filter.before.as_deref().map(parse_time).transpose()?;
    let auth = basic_auth();

    let params = jaeger::SearchParams {
        service: SERVICE,
        operation: args.operation.as_deref(),
        limit: args.filter.limit,
        tags: &tags,
        start_us,
        end_us,
    };

    let traces = jaeger::search_traces(&args.remote.jaeger_url, &params, auth.as_deref()).await?;

    if traces.is_empty() {
        eprintln!("No traces found.");
        return Ok(());
    }

    for trace in &traces {
        let root_span = trace
            .spans
            .iter()
            .find(|s| s.parent_id().is_none())
            .or_else(|| trace.spans.first());

        if let Some(span) = root_span {
            let dur_ms = span.duration as f64 / 1000.0;
            let ts_secs = span.start_time / 1_000_000;
            let datetime = format_epoch_secs(ts_secs);
            let n_spans = trace.spans.len();
            println!(
                "{} | {} | {:.1}ms | {} spans | {}",
                trace.trace_id, datetime, dur_ms, n_spans, span.operation_name,
            );
        }
    }

    Ok(())
}
