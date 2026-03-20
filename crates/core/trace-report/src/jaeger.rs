use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Trace {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    pub spans: Vec<Span>,
    pub processes: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub warnings: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    #[serde(rename = "spanID")]
    pub span_id: String,
    pub operation_name: String,
    #[serde(default)]
    pub references: Vec<Reference>,
    /// Microseconds since epoch
    pub start_time: u64,
    /// Duration in microseconds
    pub duration: u64,
    #[serde(default)]
    pub tags: Vec<Tag>,
    #[serde(default)]
    pub logs: Vec<serde_json::Value>,
    #[serde(rename = "processID")]
    pub process_id: String,
    #[serde(default)]
    pub warnings: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reference {
    pub ref_type: String,
    #[serde(rename = "traceID")]
    pub trace_id: String,
    #[serde(rename = "spanID")]
    pub span_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    #[serde(rename = "type")]
    pub tag_type: String,
    pub value: serde_json::Value,
}

impl Span {
    pub fn parent_id(&self) -> Option<&str> {
        self.references
            .iter()
            .find(|r| r.ref_type == "CHILD_OF")
            .map(|r| r.span_id.as_str())
    }
}

impl Trace {
    /// Return a new trace containing only spans in the subtrees rooted at spans
    /// whose `operation_name` matches any of `root_names`. Walks both CHILD_OF
    /// and FOLLOWS_FROM references to collect descendants.
    pub fn filter_to_subtrees(&self, root_names: &[&str]) -> Trace {
        use std::collections::HashSet;

        // Build parent → children map (follows both CHILD_OF and FOLLOWS_FROM)
        let mut children: HashMap<&str, Vec<&Span>> = HashMap::new();
        for span in &self.spans {
            for reference in &span.references {
                children
                    .entry(reference.span_id.as_str())
                    .or_default()
                    .push(span);
            }
        }

        // Find root spans
        let roots: Vec<&Span> = self
            .spans
            .iter()
            .filter(|s| root_names.contains(&s.operation_name.as_str()))
            .collect();

        // Collect all descendant IDs
        let mut keep: HashSet<&str> = HashSet::new();

        fn collect_ids<'a>(
            span: &'a Span,
            children: &HashMap<&str, Vec<&'a Span>>,
            keep: &mut HashSet<&'a str>,
        ) {
            keep.insert(&span.span_id);
            if let Some(kids) = children.get(span.span_id.as_str()) {
                for kid in kids {
                    collect_ids(kid, children, keep);
                }
            }
        }

        for root in &roots {
            collect_ids(root, &children, &mut keep);
        }

        Trace {
            trace_id: self.trace_id.clone(),
            spans: self
                .spans
                .iter()
                .filter(|s| keep.contains(s.span_id.as_str()))
                .cloned()
                .collect(),
            processes: self.processes.clone(),
            warnings: self.warnings.clone(),
        }
    }
}

/// Jaeger API response wraps traces in a `data` array.
#[derive(Debug, Deserialize)]
struct ApiResponse {
    data: Vec<Trace>,
}

fn build_client(basic_auth: Option<&str>) -> Result<(reqwest::Client, Option<(&str, &str)>)> {
    let client = reqwest::Client::new();
    let auth = match basic_auth {
        Some(auth) => {
            let (user, pass) = auth
                .split_once(':')
                .context("--basic-auth must be user:password")?;
            Some((user, pass))
        }
        None => None,
    };
    Ok((client, auth))
}

fn apply_auth(req: reqwest::RequestBuilder, auth: Option<(&str, &str)>) -> reqwest::RequestBuilder {
    match auth {
        Some((user, pass)) => req.basic_auth(user, Some(pass)),
        None => req,
    }
}

/// Search parameters for the Jaeger API.
pub struct SearchParams<'a> {
    pub service: &'a str,
    pub operation: Option<&'a str>,
    pub limit: u32,
    /// Tag filters as key=value pairs (encoded as JSON for Jaeger)
    pub tags: &'a [(String, String)],
    /// Start time in microseconds since epoch
    pub start_us: Option<u64>,
    /// End time in microseconds since epoch
    pub end_us: Option<u64>,
}

pub async fn search_traces(
    jaeger_url: &str,
    params: &SearchParams<'_>,
    basic_auth: Option<&str>,
) -> Result<Vec<Trace>> {
    let mut url = format!(
        "{}/api/traces?service={}&limit={}",
        jaeger_url.trim_end_matches('/'),
        params.service,
        params.limit,
    );
    if let Some(op) = params.operation {
        url.push_str(&format!("&operation={op}"));
    }
    if !params.tags.is_empty() {
        let tags_json: HashMap<&str, &str> = params
            .tags
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let tags_str = serde_json::to_string(&tags_json)?;
        url.push_str(&format!("&tags={}", urlencoding::encode(&tags_str)));
    }
    match (params.start_us, params.end_us) {
        (Some(start), Some(end)) => {
            url.push_str(&format!("&start={start}&end={end}"));
        }
        (Some(start), None) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            url.push_str(&format!("&start={start}&end={now}"));
        }
        _ => {
            // No time params: let Jaeger use its own default
        }
    }

    let (client, auth) = build_client(basic_auth)?;
    let resp = apply_auth(client.get(&url), auth)
        .send()
        .await
        .with_context(|| format!("searching {url}"))?;

    if !resp.status().is_success() {
        bail!(
            "Jaeger API returned {}: {}",
            resp.status(),
            resp.text().await?
        );
    }

    let api_resp: ApiResponse = resp.json().await?;
    Ok(api_resp.data)
}

pub async fn fetch_trace(
    jaeger_url: &str,
    trace_id: &str,
    basic_auth: Option<&str>,
) -> Result<Trace> {
    let url = format!(
        "{}/api/traces/{}",
        jaeger_url.trim_end_matches('/'),
        trace_id
    );
    let (client, auth) = build_client(basic_auth)?;
    let resp = apply_auth(client.get(&url), auth)
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?;

    if !resp.status().is_success() {
        bail!(
            "Jaeger API returned {}: {}",
            resp.status(),
            resp.text().await?
        );
    }

    let api_resp: ApiResponse = resp.json().await?;
    api_resp
        .data
        .into_iter()
        .next()
        .context("no trace found in Jaeger response")
}
