Generate performance reports from OpenTelemetry traces.

Connects to a Jaeger-compatible API (including VictoriaTraces) to search for traces,
filter to relevant spans, and produce flamegraphs, folded stacks, and span trees.

Designed to work with Amp's continuous jobs where outer spans (materialize_table,
streaming_query_execute) stay open indefinitely. Reports target inner spans that
complete on each processing cycle.

Report types:
  query        — Flight SQL or JSONL query execution (roots: do_get)
  derived-dataset — Derived dataset materialization (roots: execute_microbatch, close, etc.)
  raw-dataset  — Raw dataset extraction (roots: run_range)

Each report produces four artifacts:
  {prefix}_trace.json.gz   Filtered trace in Jaeger JSON format
  {prefix}_wallclock.svg   Wall-clock flamegraph
  {prefix}_busy.svg        Busy-time (CPU/poll) flamegraph
  {prefix}_folded.txt      Folded stacks for speedscope or other tools

Examples:
  # Profile a specific query by request ID
  ampctl trace report query --filter request_id=49f92800e755f99c -o query_report/

  # Profile a derived dataset job by job ID
  ampctl trace report derived-dataset --filter job_id=647 --after 5m -o dataset_report/
