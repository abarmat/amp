use std::collections::HashMap;

use crate::jaeger;

pub fn print_tree(trace: &jaeger::Trace) {
    let mut children: HashMap<&str, Vec<&jaeger::Span>> = HashMap::new();
    for span in &trace.spans {
        for reference in &span.references {
            children
                .entry(reference.span_id.as_str())
                .or_default()
                .push(span);
        }
    }
    for kids in children.values_mut() {
        kids.sort_by_key(|s| s.start_time);
    }

    let span_ids: std::collections::HashSet<&str> =
        trace.spans.iter().map(|s| s.span_id.as_str()).collect();
    let roots: Vec<&jaeger::Span> = trace
        .spans
        .iter()
        .filter(|s| {
            s.references.is_empty()
                || s.references
                    .iter()
                    .all(|r| !span_ids.contains(r.span_id.as_str()))
        })
        .collect();

    for root in roots {
        show(root, &children, 0);
    }
}

fn tag_u64(span: &jaeger::Span, key: &str) -> Option<u64> {
    span.tags.iter().find(|t| t.key == key).and_then(|t| {
        t.value
            .as_u64()
            .or_else(|| t.value.as_i64().map(|v| v as u64))
    })
}

fn show(span: &jaeger::Span, children: &HashMap<&str, Vec<&jaeger::Span>>, indent: usize) {
    let mut label = span.operation_name.clone();
    for tag in &span.tags {
        if matches!(tag.key.as_str(), "start_block" | "end_block" | "query")
            && let Some(v) = tag.value.as_str()
        {
            let v = if v.len() > 50 { &v[..50] } else { v };
            label.push_str(&format!(" {}={}", tag.key, v));
        }
    }

    let dur_ms = span.duration as f64 / 1000.0;
    let busy_ns = tag_u64(span, "busy_ns");
    let busy_str = busy_ns
        .map(|b| format!(", busy={:.1}ms", b as f64 / 1_000_000.0))
        .unwrap_or_default();

    let kids = children.get(span.span_id.as_str());
    let kids_dur: u64 = kids
        .map(|k| k.iter().map(|s| s.duration).sum())
        .unwrap_or(0);
    let overflow = if kids_dur > span.duration {
        " !!OVERFLOW"
    } else {
        ""
    };
    let self_dur = span.duration.saturating_sub(kids_dur) as f64 / 1000.0;

    println!(
        "{:indent$}{label} ({dur_ms:.1}ms, self={self_dur:.1}ms{busy_str}{overflow})",
        "",
        indent = indent * 2
    );

    if let Some(kids) = kids {
        for child in kids {
            show(child, children, indent + 1);
        }
    }
}
