# Operational observability

Set `REORGGUARD_API_ADDR=127.0.0.1:8080` with live mode to serve the [OpenAPI contract](../api/openapi.yaml). Only a loopback bind is accepted. `/health/live` is process-only; `/health/ready` combines PostgreSQL checkpoint integrity and the latest safe HTTP canonical sweep. `/v1/status` shows the durable checkpoint, last committed reorg, logical provider names and live transport state. `/v1/logs` is canonical-only and page-bounded. A selected payload over 32 KiB returns HTTP 413 while its durable row remains intact. HTTP requests have a five-second deadline and 32-request concurrency cap except liveness. Error messages and request logs contain no raw driver/RPC error or query string.

`GET /metrics` emits Prometheus text. Counters reset on process restart; checkpoint, canonical revision and last reorg remain durable in PostgreSQL. If PostgreSQL cannot answer, durable checkpoint/lag/age gauges emit `NaN` rather than a misleading zero. Every label comes from fixed methods, endpoint IDs, states or reason enumerations. No hash, transaction ID, URL or request ID is used as a metric label.

| Metric | Meaning |
| --- | --- |
| `reorgguard_remote_head_block`, `reorgguard_indexed_head_block`, `reorgguard_lag_blocks` | Last observed HTTP head, durable checkpoint and their nonnegative difference |
| `reorgguard_checkpoint_age_seconds` | Seconds since the checkpoint's last durable update |
| `reorgguard_backfill_range_blocks` | Size of the latest successfully committed historical range |
| `reorgguard_logs_processed_total` | Filtered log observations in successfully committed backfill ranges; includes repeated identical observations |
| `reorgguard_duplicate_logs_total` | Identical repeated logs collapsed during those committed range validations |
| `reorgguard_reorgs_total`, `reorgguard_reorg_depth_blocks` | Committed reorg count and latest committed depth |
| `reorgguard_orphaned_blocks_total`, `reorgguard_orphaned_logs_total`, `reorgguard_common_ancestor_height`, `reorgguard_reconciliation_failures_total{reason}` | Reorg audit counters, latest ancestor height and finite failure categories |
| `reorgguard_rpc_requests_total{endpoint,method,status}`, `reorgguard_rpc_latency_seconds{endpoint,method}` | RPC call count and last observed latency by logical endpoint and fixed method |
| `reorgguard_rpc_endpoint_state{endpoint,state}`, `reorgguard_rpc_endpoint_selected{endpoint}`, `reorgguard_rpc_endpoint_head{endpoint}` | Provider health, selection and reported head; head exists when a pool is configured |
| `reorgguard_rpc_failovers_total`, `reorgguard_rpc_failover_reasons_total{reason}`, `reorgguard_rpc_incompatibilities_total`, `reorgguard_rpc_stale_detections_total` | Guarded provider changes and reasons; reason series exist with a pool |
| `reorgguard_rpc_adaptive_window_blocks{endpoint}`, `reorgguard_rpc_provider_limit_responses_total{endpoint}` | Per-provider range capability; a pool is required |
| `reorgguard_active_backfill_workers{endpoint}`, `reorgguard_inflight_backfill_ranges{endpoint}`, `reorgguard_pending_ordered_results{endpoint}`, `reorgguard_backfill_backpressure_total{endpoint}` | Bounded parallel fetch occupancy and backpressure |
| `reorgguard_ws_connected`, `reorgguard_ws_reconnects_total`, `reorgguard_ws_reconnect_backoff_seconds`, `reorgguard_ws_hints_total`, `reorgguard_ws_duplicate_or_stale_hints_total`, `reorgguard_ws_removed_hints_total` | WS connection and hint activity |
| `reorgguard_http_catchup_ranges_total`, `reorgguard_polling_fallback_total`, `reorgguard_live_lag_blocks`, `reorgguard_live_transport_failures_total{reason}`, `reorgguard_live_ready` | HTTP recovery and live readiness |

Go `slog` records fixed operation names, logical endpoint IDs, bounded failure reasons and reorg old head → ancestor → new head identities. It never records configured URLs. Set `REORGGUARD_TRACING=stdout` for local JSON OpenTelemetry spans, or `off` (default) for a no-op tracer. Parent context flows from live/API operation through pool selection, HTTP RPC, range validation, canonical transaction and checkpoint update. Spans carry bounded operation/method/block-count attributes and finite error categories; no DSN, URL, response body, secret or raw error text. The local demo enables stdout tracing in its temporary process log; no external collector is required.
