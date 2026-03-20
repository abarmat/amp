//! Integration tests for segment-level _block_num filter pruning with Anvil.
//!
//! Creates two segments (blocks 0-3 and 4-7) then verifies that _block_num
//! filters prune segments correctly by checking both result correctness and
//! EXPLAIN output for reduced file group counts.

use monitoring::logging;

use crate::{steps::run_spec, testlib::ctx::TestCtxBuilder};

/// Runs the YAML spec that validates result correctness for various _block_num filters,
/// then checks EXPLAIN output to verify that segment pruning reduces file groups.
#[tokio::test(flavor = "multi_thread")]
async fn segment_pruning() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("segment_pruning")
        .with_anvil_ipc()
        .with_dataset_manifest("anvil_rpc")
        .build()
        .await
        .expect("Failed to create test environment");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    // Run the full spec: sets up two segments (blocks 0-3 and 4-7) and
    // validates result correctness for various _block_num filters.
    run_spec("segment-pruning-anvil", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run segment pruning spec");

    // Now verify EXPLAIN output to confirm segment pruning reduces file groups.

    // Baseline: no filter should see file_groups from both segments.
    let baseline_plan = explain(&mut client, "SELECT block_num FROM anvil_rpc.blocks").await;
    let baseline_groups = count_file_groups(&baseline_plan);
    assert!(
        baseline_groups > 0,
        "Baseline should have file groups.\nplan: {baseline_plan}"
    );

    // Filtered: _block_num > 3 should only touch the second segment.
    let filtered_plan = explain(
        &mut client,
        "SELECT block_num FROM anvil_rpc.blocks WHERE _block_num > 3",
    )
    .await;
    let filtered_groups = count_file_groups(&filtered_plan);

    assert!(
        filtered_groups < baseline_groups,
        "Expected fewer file groups with _block_num filter.\n\
         baseline groups: {baseline_groups}\n\
         filtered groups: {filtered_groups}\n\
         baseline plan: {baseline_plan}\n\
         filtered plan: {filtered_plan}"
    );

    // Out-of-range filter should produce an empty plan (no file groups).
    let empty_plan = explain(
        &mut client,
        "SELECT block_num FROM anvil_rpc.blocks WHERE _block_num > 100",
    )
    .await;
    let empty_groups = count_file_groups(&empty_plan);

    assert_eq!(
        empty_groups, 0,
        "Expected zero file groups for out-of-range filter.\nplan: {empty_plan}"
    );
}

/// Runs EXPLAIN on a query and returns the plan text.
async fn explain(client: &mut crate::testlib::fixtures::FlightClient, query: &str) -> String {
    let explain_query = format!("EXPLAIN {query}");
    let (json, _) = client
        .run_query(&explain_query, None)
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN query failed: {e}\nquery: {explain_query}"));

    // EXPLAIN returns rows with "plan_type" and "plan" columns.
    match json {
        serde_json::Value::Array(rows) => rows
            .iter()
            .filter_map(|row| row.get("plan").and_then(|v| v.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("Unexpected EXPLAIN output format: {other:?}"),
    }
}

/// Counts the number of file groups in an EXPLAIN plan.
///
/// Looks for the `file_groups={N group` pattern in the plan text.
/// Returns 0 if the pattern is not found (e.g. EmptyExec plans).
fn count_file_groups(plan: &str) -> usize {
    // Pattern: "file_groups={14 groups:" or "file_groups={1 group:"
    let prefix = "file_groups={";
    let Some(start) = plan.find(prefix) else {
        return 0;
    };
    let after = &plan[start + prefix.len()..];
    let end = after.find(' ').unwrap_or(after.len());
    after[..end].parse().unwrap_or(0)
}
