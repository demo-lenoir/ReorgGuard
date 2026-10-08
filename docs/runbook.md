# Operations runbook

Status: release runbook for one-shot, long-running live, guarded multi-RPC, and loopback operational API modes. Deep or unprovable forks require explicit operator review.

## Operational checks

With `REORGGUARD_API_ADDR=127.0.0.1:8080` in live mode, read `/v1/status` for the durable checkpoint, reported head, lag, last reorg, selected logical provider and bounded provider/WS states. `/health/live` reports process liveness even during dependency outages. `/health/ready` returns 503 when PostgreSQL is unavailable, safe HTTP progress is impossible, the canonical branch is unsafe, or lag exceeds tolerance. An isolated WS outage can remain ready while HTTP polling works. `/metrics` exposes fixed-label gauges/counters; see [metric meanings](observability.md). `/v1/logs?from_block=...&to_block=...&limit=50` reads canonical logs; its cursor returns 409 after a reorg and traversal must restart. This API is loopback-only and unauthenticated.

If a proven unsafe fork halts live sync, the API-enabled process remains alive for inspection but `/health/ready` returns 503. It cannot resume until an operator resolves the durable unsafe marker. During an HTTP or PostgreSQL outage, liveness remains 200 and readiness becomes unhealthy; after recovery, the next poll rereads the durable checkpoint. No URL or API key is returned in status or metrics. Set `REORGGUARD_TRACING=stdout` temporarily to inspect bounded JSON spans locally; turn it back to `off` when unneeded.

## Indexer crashed or was SIGKILLed

1. Keep the existing PostgreSQL database and configuration. Do not reset `sync_state`, delete orphaned rows, or rebuild the schema as a routine recovery step.
2. Verify PostgreSQL is available. Read `sync_state.checkpoint_number`, `checkpoint_hash`, and `fail_reason` for the configured chain. Join the checkpoint to `blocks` and require `canonical=true` at that height and hash. If the join fails, stop and investigate the database; normal indexing must not proceed.
3. Restart the same binary with the same chain ID, genesis/anchor, start block, filter, RPC endpoint, and database. It revalidates the network and durable checkpoint. It validates the complete durable checkpoint identity, then fetches from `checkpoint+1` or performs bounded reorg reconciliation if the provider proves old history by hash and reports a new branch.
4. Confirm the new checkpoint names a canonical block, canonical heights are contiguous, and canonical log rows belong to canonical block hashes. Compare expected head/log counts for the local fixture or operational source. The crash test harness also compares an ordered SHA-256 checksum against an uninterrupted reference run.

Read-only checkpoint inspection (substitute the configured numeric chain ID):

```sql
SELECT st.chain_id, st.checkpoint_number, encode(st.checkpoint_hash, 'hex') AS checkpoint_hash,
       st.fail_reason, b.canonical
FROM sync_state st
LEFT JOIN blocks b ON b.chain_id = st.chain_id
                  AND b.number = st.checkpoint_number
                  AND b.hash = st.checkpoint_hash
WHERE st.chain_id = 31337;
```

`b.canonical` must be `true`. A missing row or `false` requires investigation before restart.

An exit directly after commit may leave a checkpoint ahead of the process's final success log. The database checkpoint is authoritative. An exit before commit leaves the previous checkpoint and branch intact; PostgreSQL rolls back the interrupted transaction.

## SIGTERM versus SIGKILL

SIGTERM cancels RPC/DB work and allows up to five seconds for the process to finish shutdown. A canceled uncommitted transaction must not move the checkpoint. If shutdown exceeds that deadline, the process exits forcibly. SIGKILL gives no cleanup opportunity; recovery relies on PostgreSQL transaction atomicity and startup validation. The crash suite tests both signals at deterministic boundaries.

## PostgreSQL temporarily unavailable

Readiness is unhealthy while PostgreSQL cannot be queried. The current one-shot invocation exits after bounded DB work rather than retrying indefinitely. Restore the same PostgreSQL database, then restart the binary with unchanged dataset identity. Inspect the checkpoint before and after restart. The Docker-pause test observed a bounded failure of about 25 seconds under current timeouts and no checkpoint movement.

## Unsafe or deep fork

If `sync_state.fail_reason` is set, do not clear it or force a checkpoint change merely to resume. Preserve a database backup and the observed chain headers, determine the correct network history and retention boundary, and plan an explicit reviewed recovery. The current executable intentionally refuses automatic progression from a durable unsafe marker. A supported operator repair command is not implemented yet.

## WebSocket outage or repeated reconnect

In live mode (`REORGGUARD_SYNC_MODE=live`), WS notifications only wake an HTTP sweep. Check that HTTP RPC and PostgreSQL remain available and that the durable checkpoint advances during periodic polling. An isolated WS outage is a degraded transport, not an unsafe canonical state. Reconnect attempts use capped exponential backoff with jitter; repeated attempts are expected while the endpoint is absent. Inspect internal `Status`/fixed-series metrics in tests or process instrumentation until the operational API exists. Do not clear the checkpoint or orphan rows to repair a subscription.

After WS returns, a fresh connection verifies chain ID/genesis, subscribes, and immediately triggers an HTTP sweep from the durable checkpoint boundary. Blocks generated during the outage are fetched and committed through the existing canonical writer. The first live sweep after reconnect may take more than one bounded range. A malformed WS payload closes that connection and retries; a wrong WS network anchor is a terminal configuration error and must be corrected before restart.

## HTTP fallback and both transports unavailable

Set a bounded `REORGGUARD_POLL_INTERVAL` (default `3s`; allowed `10ms`–`1h`) for live mode. Polling continues with WS absent. If every HTTP endpoint fails, WS hints cannot advance the database: readiness becomes `http_unavailable`, and the next interval retries from PostgreSQL state. If both transports fail, restore HTTP first; then confirm the checkpoint catches up. Persistent `unsafe` readiness or `sync_state.fail_reason` requires the deep-fork procedure above.

## HTTP provider outage and failover

Configure up to three comma-separated fallback URLs in `REORGGUARD_RPC_FALLBACK_URLS`. Endpoint names in internal status/metrics are `primary`, `fallback_1`, and so on; URLs and credentials must stay in protected process configuration. On primary timeout or transport failure, the active sweep reads the durable checkpoint and validates another endpoint's chain ID, genesis, head, checkpoint header and required methods before using it. Confirm that the selected endpoint changes and that the checkpoint continues from its stored height. Do not reset the checkpoint when switching. A WS outage remains nonfatal if one safe HTTP endpoint progresses.

If a provider rate-limits `eth_getLogs`, inspect its limit count and adaptive window. The same range start is retried with a smaller window. A different provider retains its own window. Persistent limits at the minimum range make that sweep fail within the attempt/deadline bound; improve provider capacity or configuration, then retry from PostgreSQL. Repeated endpoint flapping is damped by cooldown and the finite switch budget; if all candidates are cooling down, the next poll retries after the configured delay. Inspect `/v1/status` or `Pool.Status` for endpoint health and the current adaptive window.

For a stale provider, compare its reported head with `sync_state.checkpoint_number`. An endpoint behind the checkpoint cannot be selected; a provider behind another eligible source may be reported `stale`. Wait for it to catch up or use a compatible fallback. If all providers are unavailable, readiness is unhealthy and no checkpoint moves. Recover a provider and verify the next sweep resumes at `checkpoint+1`.

A wrong-chain, wrong-genesis, or wrong configured start-anchor endpoint is hard rejected. Correct its configuration and restart; do not force it eligible. A provider that cannot reproduce the durable checkpoint block and completed filtered-log set by hash is ineligible for that checkpoint, even with the right chain ID/genesis. This checkpoint-relative rejection is re-evaluated after a canonical checkpoint change. Missing required methods, wrong chain, wrong genesis and wrong configured anchor remain process-lifetime hard failures. A provider that reports a fork after restart must first reproduce the old checkpoint by hash; the same bounded common-ancestor procedure then decides whether its branch can be committed. If no ancestor can be proven, use the unsafe/deep-fork procedure above. Multiple endpoint agreement is never proof of canonicality. Within one failover sweep, a provider that failed canonical work is not tried again; cooldown governs later sweeps.

## Release verification

Run `make verify` from a committed checkout with Go 1.27.1, Foundry 1.8.4, Docker, Python 3, `psql`, `rg`, govulncheck 1.8.0, actionlint 1.7.12, Trivy 0.75.0 and Syft 1.54.0 installed. The gate creates and removes temporary PostgreSQL containers and Anvil processes; it then clones the committed repository into a temporary directory and repeats the gate there. If benchmark evidence validation fails, rerun `make benchmark` on a committed implementation revision and inspect `docs/benchmarks/raw.json` before accepting new results. Never copy a throughput number from a failed run.

The reference image binds its operational API to loopback inside the container. Test it with `scripts/verify-image.py`, which creates only temporary local dependencies and joins the container network namespace for API probes. A failed scan should be triaged at the reported dependency/image version; do not suppress a finding to make the release gate pass. Generated local provenance is unsigned metadata, so an operator must not present it as a signed supply-chain attestation.

## Filter mismatch and slow API clients

An out-of-filter `eth_getLogs` row causes the HTTP range or replacement fetch to fail without discarding the row silently or advancing the checkpoint. Check the configured address/topic filter and provider response; a plausible omission of a matching log remains outside what RPC responses alone can prove. The loopback operational server has both a five-second request context and a five-second socket write deadline. If readiness/status requests return 429, inspect slow local readers or a trusted proxy as well as database latency.
