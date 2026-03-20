Environment Variables:
    AMP_LOG
        Controls logging verbosity for all ampctl operations.
        Valid values: error, warn, info, debug, trace (default: info)

    JAEGER_URL
        Jaeger-compatible API base URL for trace commands.
        For VictoriaTraces, include the /select/jaeger prefix.
        Default: http://localhost:16686

    JAEGER_AUTH
        Basic auth credentials (user:password) for the Jaeger API.
        Required when the traces endpoint is behind authentication.

Examples:
    # Run with debug logging to see detailed operation flow
    AMP_LOG=debug ampctl manifest register edgeandnode/eth@1.0.0 ./manifest.json

    # Run with minimal logging (errors only)
    AMP_LOG=error ampctl manifest register edgeandnode/eth@1.0.0 s3://manifests/eth.json

    # Run with trace logging for maximum verbosity
    AMP_LOG=trace ampctl manifest register graph/mainnet@2.0.0 gs://data/manifest.json
