use std::{collections::HashMap, fs::File, io::BufWriter, path::Path};

use anyhow::{Context, Result};
use inferno::flamegraph::{self, Options};

use super::jaeger::{Span, Trace};

pub struct FlamegraphConfig {
    pub min_duration_us: u64,
    /// Use `busy_ns` tag (active/poll time) instead of wall-clock `duration`.
    /// This is more accurate for async code where spans can be idle while awaiting.
    /// Falls back to `duration` if `busy_ns` is not present.
    pub use_busy_time: bool,
}

/// Generate a flamegraph SVG from a Jaeger trace.
pub fn generate(trace: &Trace, output: &Path, config: &FlamegraphConfig) -> Result<()> {
    let folded = build_folded(trace, config)?;

    let mut opts = Options::default();
    opts.title = make_title(trace, config);
    opts.count_name = "μs".to_string();
    opts.min_width = 0.1;

    let file = File::create(output).with_context(|| format!("creating {}", output.display()))?;
    let mut writer = BufWriter::new(file);
    flamegraph::from_lines(&mut opts, folded.iter().map(|s| s.as_str()), &mut writer)
        .context("generating flamegraph SVG")?;

    Ok(())
}

/// Build folded stacks and return them as lines (for dump mode or flamegraph).
pub fn build_folded(trace: &Trace, config: &FlamegraphConfig) -> Result<Vec<String>> {
    let children = build_children_map(trace);

    // Root detection via orphan analysis: a span is a root if none of its
    // references point to a span present in this trace. This is the single
    // code path for all commands — works identically whether the trace is
    // complete (batch query where filter_to_subtrees removed ancestors) or
    // partial (continuous derived dataset where parent spans are still
    // open/unexported by OTel).
    let roots = find_orphan_roots(trace);

    if roots.is_empty() {
        anyhow::bail!("no root spans found in trace");
    }

    let mut folded_lines: Vec<String> = Vec::new();

    for root in &roots {
        let mut stack = Vec::new();
        build_folded_stacks(root, &children, &mut stack, &mut folded_lines, config);
    }

    if folded_lines.is_empty() {
        anyhow::bail!("no spans produced folded stacks (all filtered out?)");
    }

    Ok(folded_lines)
}

/// Find root spans via orphan detection: spans whose parent references
/// point to spans not present in this trace (or have no references at all).
pub fn find_orphan_roots(trace: &Trace) -> Vec<&Span> {
    let span_ids: std::collections::HashSet<&str> =
        trace.spans.iter().map(|s| s.span_id.as_str()).collect();
    trace
        .spans
        .iter()
        .filter(|s| {
            s.references.is_empty()
                || s.references
                    .iter()
                    .all(|r| !span_ids.contains(r.span_id.as_str()))
        })
        .collect()
}

fn build_children_map(trace: &Trace) -> HashMap<&str, Vec<&Span>> {
    let mut map: HashMap<&str, Vec<&Span>> = HashMap::new();
    for span in &trace.spans {
        // Follow both CHILD_OF and FOLLOWS_FROM so that spawned tasks
        // (e.g. streaming_query_execute) appear under their parent.
        for reference in &span.references {
            map.entry(reference.span_id.as_str())
                .or_default()
                .push(span);
        }
    }
    for kids in map.values_mut() {
        kids.sort_by_key(|s| s.start_time);
    }
    map
}

/// Get the time value to use for a span.
///
/// In `busy_time` mode: uses the `busy_ns` tag which represents actual active/poll
/// time (from the tracing crate's OpenTelemetry layer). This correctly handles async
/// spans where the span is idle while awaiting. Falls back to duration (converted to
/// ns) if the tag is missing.
///
/// Returns span time in microseconds.
fn span_time(span: &Span, use_busy_time: bool) -> u64 {
    if use_busy_time {
        let busy_ns = span.tags.iter().find(|t| t.key == "busy_ns").and_then(|t| {
            t.value
                .as_u64()
                .or_else(|| t.value.as_i64().map(|v| v as u64))
        });
        match busy_ns {
            Some(ns) => ns / 1000, // convert ns → μs
            None => span.duration, // fallback to wall-clock μs
        }
    } else {
        span.duration
    }
}

/// Recursively build folded stack lines.
///
/// In busy_time mode, each span contributes its busy_ns as self-time minus
/// children's busy_ns. Since busy_ns represents actual poll time, children
/// should not exceed parent (unlike wall-clock duration in async code).
///
/// In wall-clock mode, self-time = duration - children durations, clamped to 0.
fn build_folded_stacks<'a>(
    span: &'a Span,
    children_map: &HashMap<&str, Vec<&'a Span>>,
    stack: &mut Vec<String>,
    output: &mut Vec<String>,
    config: &FlamegraphConfig,
) {
    if span.duration < config.min_duration_us {
        return;
    }

    let time = span_time(span, config.use_busy_time);
    let label = span_label(span);
    stack.push(label);

    let kids = children_map.get(span.span_id.as_str());
    let children_time: u64 = kids
        .map(|kids| {
            kids.iter()
                .filter(|k| k.duration >= config.min_duration_us)
                .map(|k| span_time(k, config.use_busy_time))
                .sum()
        })
        .unwrap_or(0);

    let self_time = time.saturating_sub(children_time);

    if self_time > 0 {
        let path = stack.join(";");
        output.push(format!("{path} {self_time}"));
    }

    if let Some(kids) = kids {
        for child in kids {
            build_folded_stacks(child, children_map, stack, output, config);
        }
    }

    stack.pop();
}

fn span_label(span: &Span) -> String {
    let mut label = span.operation_name.clone();

    let start_block = tag_value(span, "start_block");
    let end_block = tag_value(span, "end_block");
    if let (Some(s), Some(e)) = (start_block, end_block) {
        label.push_str(&format!(" [{s}..{e}]"));
    }

    if let Some(q) = tag_value(span, "query") {
        let q_short = if q.len() > 60 { &q[..60] } else { q };
        label.push_str(&format!(" ({q_short})"));
    }

    label
}

fn tag_value<'a>(span: &'a Span, key: &str) -> Option<&'a str> {
    span.tags
        .iter()
        .find(|t| t.key == key)
        .and_then(|t| t.value.as_str())
}

fn make_title(trace: &Trace, config: &FlamegraphConfig) -> String {
    let service = trace
        .processes
        .values()
        .next()
        .and_then(|p| p.get("serviceName"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let mode = if config.use_busy_time {
        "busy time"
    } else {
        "wall clock"
    };

    format!("Trace flamegraph ({mode}) — {service}")
}
