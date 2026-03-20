pub mod flamegraph;
pub mod jaeger;
pub mod time;
pub mod tree;

use std::path::Path;

use anyhow::{Context, Result, bail};
use flamegraph::FlamegraphConfig;

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

/// Generate a full report: trace JSON, wallclock SVG, busy SVG, folded stacks.
pub fn generate_report(trace: &jaeger::Trace, output_dir: &Path, prefix: &str) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating {}", output_dir.display()))?;

    // Save trace
    let json_path = output_dir.join(format!("{prefix}_trace.json.gz"));
    save_trace_gz(trace, &json_path)?;
    eprintln!("Wrote trace to {}", json_path.display());

    // Span tree
    eprintln!("\n--- Span tree ---");
    tree::print_tree(trace);
    eprintln!("---\n");

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
pub fn load_trace(path: &Path) -> Result<jaeger::Trace> {
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
pub fn save_trace_gz(trace: &jaeger::Trace, path: &Path) -> Result<()> {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};
    let json = serde_json::to_vec(trace)?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&json)?;
    std::fs::write(path, encoder.finish()?)?;
    Ok(())
}

/// Merge multiple Jaeger traces into one.
pub fn merge_traces(traces: Vec<jaeger::Trace>) -> jaeger::Trace {
    let mut iter = traces.into_iter();
    let mut merged = iter.next().expect("at least one trace");
    for trace in iter {
        merged.spans.extend(trace.spans);
        merged.processes.extend(trace.processes);
    }
    merged
}

/// Search Jaeger and produce a filtered trace, or load from a file.
#[expect(clippy::too_many_arguments)]
pub async fn fetch_and_filter(
    jaeger_url: &str,
    basic_auth: Option<&str>,
    filters: &[(String, String)],
    after: Option<u64>,
    before: Option<u64>,
    limit: u32,
    service: &str,
    root_spans: &[&str],
    file: Option<&Path>,
) -> Result<jaeger::Trace> {
    let full_trace = match file {
        Some(path) => {
            let trace = load_trace(path)?;
            eprintln!("Loaded {} spans from {}", trace.spans.len(), path.display());
            trace
        }
        None => {
            let primary_operation = root_spans.first().copied();
            let params = jaeger::SearchParams {
                service,
                operation: primary_operation,
                limit,
                tags: filters,
                start_us: after,
                end_us: before,
            };
            let traces = jaeger::search_traces(jaeger_url, &params, basic_auth).await?;
            if traces.is_empty() {
                bail!("no traces found matching filters");
            }
            let merged = merge_traces(traces);
            eprintln!("Fetched {} spans from Jaeger", merged.spans.len());
            merged
        }
    };

    let filtered = full_trace.filter_to_subtrees(root_spans);
    eprintln!(
        "Filtered to {} spans (roots: {:?})",
        filtered.spans.len(),
        root_spans,
    );
    Ok(filtered)
}
