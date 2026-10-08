# ReorgGuard implementation contract

Status: implemented release contract. Publication does not imply production deployment or operating history. Current official upstream behavior takes priority if it conflicts with this contract; record any deviation in an ADR.

## Purpose and scope

Index a configured set of EVM contract addresses/topics into PostgreSQL while preserving a queryable canonical view through provider limits, process restarts, dropped WebSocket connections, duplicate delivery, and bounded chain reorganizations. One process handles one configured chain; `chain_id` remains part of persisted identity. The first release uses local Anvil for deterministic end-to-end evidence. It sends no public-network transaction.

Required components: HTTP JSON-RPC historical backfill with adaptive `eth_getLogs` ranges; WebSocket head/log hints with HTTP gap recovery; canonical blocks and immutable log identity; transactional checkpoint and reorg reconciliation; bounded fetch concurrency and queues; multiple RPC endpoint health and guarded failover; PostgreSQL; small OpenAPI-defined read/status API; Prometheus, OTel traces, structured logs; deterministic failure tests; reproducible benchmark; non-root container, supply-chain checks, and clean-clone demo.

Excluded: explorer UI, traces, archive node, multi-chain framework, broker/cache/analytics infrastructure, generic indexing plugins, public deployment or transactions.

## Configuration and starting point

Required: expected chain ID, genesis hash or equivalent trusted network anchor, HTTP endpoints, optional WS endpoint, filter (addresses/topics), start block, PostgreSQL DSN, and max reorg depth. Versioned defaults and bounded limits are required for initial/min/max backfill range, attempt count, per-call and total range deadlines, retry backoff, poll interval, worker count, queue capacity, max in-flight ranges, DB pool, API page limit, and readiness lag tolerance. Invalid or unbounded values fail startup.

For `start_block > 0`, persist and validate the header at `start_block - 1` as the anchor before committing `start_block`. Genesis is the anchor for a zero or one start. For `start_block > 1`, the expected anchor hash must also be supplied independently of the RPC response. A checkpoint is a contiguous canonical coverage frontier for the configured filter, including empty blocks. Changing the filter or network anchor requires a new dataset or explicit migration/rebuild; existing checkpoints cannot be silently reused.

## Invariants

| ID | Requirement |
| --- | --- |
| I01 | At most one canonical hash exists per `(chain_id, height)`; every canonical height from the anchor through checkpoint has exactly one block. |
| I02 | Every canonical log belongs to the canonical block at its height and matches its block hash; reads return canonical logs only. |
| I03 | Repeated delivery creates no second canonical log; identical identity/payload is idempotent. |
| I04 | Checkpoint advances only in the transaction that durably persists and canonicalizes all corresponding headers and filtered logs. |
| I05 | Restart from the durable checkpoint creates no gap and no duplicate canonical record. |
| I06 | WebSocket is an acceleration signal only. HTTP header/log catch-up from the durable checkpoint runs on startup, reconnect, periodic poll, and suspicious notifications. |
| I07 | A fork is accepted only after a parent-linked common ancestor is proven against retained local canonical headers, within max reorg depth. |
| I08 | A fork beyond policy, missing ancestor, corrupt parent chain, or unresolved provider disagreement fails closed; readiness stays unhealthy until operator action. |
| I09 | A previously orphaned branch may return to canonical status if and only if its previously stored block and filtered-log identities and payloads match exactly. |
| I10 | Changed block fields for an existing `(chain_id, hash)` or changed log payload/set for a previously completed block are rejected and reported. |
| I11 | Concurrent fetches may complete out of order, but only one ordered canonical writer advances contiguous coverage. |
| I12 | Adaptive range retries keep the same starting height, are bounded by attempts and deadline, and never skip or durably overlap coverage. |
| I13 | Automatic RPC failover requires matching chain ID, network anchor, and a validated checkpoint hash/history boundary; no provider pair is treated as a consensus oracle. |
| I14 | Cancellation, process death, DB error, or shutdown leaves either the old or complete new canonical transaction state. |
| I15 | Readiness is false when canonical progress cannot be established safely, including deep fork, DB outage, endpoint incompatibility, or excessive lag. |

## Data and transaction contract

`blocks` has `(chain_id, hash)` identity, height, parent hash, timestamp, canonical flag, first seen time, and a completed filtered-log-set fingerprint/count. `logs` has immutable `(chain_id, block_hash, tx_hash, log_index)` identity and block number, transaction index, address, topics, and data. `sync_state` holds chain ID, anchor/filter fingerprint, canonical checkpoint number/hash, optional observed safe head, updated time, and fail-closed state. Schema constraints enforce hash lengths, nonnegative heights/indexes, FK from logs to blocks, unique canonical height via partial unique index, and one sync row per chain. Immutable identity is verified on conflict; `ON CONFLICT DO NOTHING` alone is insufficient. Canonical log visibility derives from the block's canonical flag; a WebSocket `removed=true` event is a reconciliation hint (ADR-0002).

Before a canonical write, fetch and validate complete headers and the filtered `eth_getLogs` result for every covered height, including empty ones. Check `blockHash`, `blockNumber`, log order/uniqueness, and parent linkage. A transaction locks `sync_state`, verifies the expected old checkpoint, marks losing blocks noncanonical, inserts or reactivates exact-match branch data, marks the selected branch canonical in height order, and updates the checkpoint last. A branch switch through its new head is atomic. Later catch-up blocks may use separate contiguous transactions. No network call occurs while holding this transaction. A failed or interrupted transaction rolls back completely. The query API uses the same committed canonical view.

## Backfill behavior

For each next uncovered `from`, choose `to = min(from + window - 1, target)`. Fetch via HTTP with context/deadline. Locally validate every returned log against the configured address and positional topic predicates, including OR alternatives and wildcard positions; a mixed response containing an out-of-filter row fails without advancing the checkpoint. On explicitly classified provider range/result-limit error, shrink the window (never below min) and retry from the same `from` with a recomputed `to`. At minimum window, failure is terminal after the configured attempt/deadline bound. Network/server errors use bounded backoff, endpoint health updates, and the same `from`. Unknown error codes are not assumed to mean a range limit. Grow only after a configurable run of successful, small, healthy-latency ranges. A successful range commits exactly `[from,to]`; only then does `from` become `to+1`. Multi-worker fetching is allowed only with bounded buffering and ordered commit.

## Live, reorg, and provider behavior

Subscribe to heads and optionally logs. Notifications only trigger a common HTTP reconciliation path; duplicate heads/logs and `removed=true` do not directly rewrite canonical rows. On WS loss, continue HTTP polling; reconnect performs an HTTP sweep from the durable checkpoint boundary. HTTP response inconsistency, provider staleness, and endpoint method limitations affect health. Wrong-chain endpoints are rejected. Automatic failover cannot switch to an endpoint with a different anchor or checkpoint hash. A verified hash mismatch at the checkpoint initiates bounded ancestor search on the active trustworthy history; disagreement without a defensible history is fail-closed, not a majority vote. Configured max depth is inclusive: exactly `N` may reconcile; `N+1` fails. Optional safe-head reporting is observational and may move backward; it cannot override canonical validation.

## API and observability

OpenAPI defines `GET /v1/status`, `GET /v1/logs`, `GET /health/live`, `GET /health/ready`, and `GET /metrics`. Logs use stable keyset pagination with maximum 100 rows and filters for block range/address/topic0; no unbounded list. A page cursor names the canonical revision and is rejected if a reorg changes that revision between pages. A selected log payload above 32 KiB returns HTTP 413 while its durable row remains intact. Errors have stable code, message, and request ID. Readiness checks DB, fail-closed state, endpoint compatibility, and configured lag tolerance. API has a five-second request deadline and a 32-request concurrency cap. Secrets and complete RPC URLs never appear in API, logs, metrics, or traces.

Required metrics: `reorgguard_remote_head_block`, `reorgguard_indexed_head_block`, `reorgguard_lag_blocks`, `reorgguard_backfill_range_blocks`, `reorgguard_logs_processed_total`, `reorgguard_duplicate_logs_total`, `reorgguard_reorgs_total`, `reorgguard_reorg_depth_blocks`, `reorgguard_rpc_requests_total{endpoint,method,status}`, `reorgguard_rpc_latency_seconds{endpoint,method}`, and `reorgguard_checkpoint_age_seconds`. Endpoint labels are bounded configured IDs, not URLs. Structured reorg/failure events include chain ID, heights, hashes, endpoint ID, operation/request ID, and reason. Traces cover RPC fetch, validation, reconciliation, and DB commit with bounded attributes.

## Observable acceptance matrix

| Gate | Required observable proof |
| --- | --- |
| Storage | PostgreSQL migration tests show uniqueness/FK/checkpoint constraints; injected error or cancellation before commit leaves old block/log/checkpoint snapshot; after commit restart sees all rows. |
| Adaptive backfill | Table-driven deterministic range evolution including limit errors of multiple provider formats, min-window failure, deadline/cancellation, growth, empty blocks, and exact contiguous checkpoint coverage. |
| Canonicality | Deterministic clean append; forks of depth 1, 2, and configurable N; exactly N succeeds and N+1 fails; A→B→A reactivation and derived terminal records; same-height competition; missing ancestor; corrupted parents; changed block/log-set identity; safe-head retreat when relevant. |
| Restart/chaos | Kill mid-batch and during reorg; restart yields no gap/duplicate. DB outage and cancellation produce bounded failure and false readiness. |
| Live | Drop WS, create events, reconnect, and prove HTTP catch-up; repeated head/log delivery and `removed=true` converge with pure backfill. |
| Concurrency | Bounded workers/queues and backpressure; overlapping out-of-order fetch completion commits contiguously; repeated start/stop, worker error, in-flight shutdown, and `go test -race` pass. |
| RPC pool | Wrong-chain hard reject; stale-head health degradation; transport/latency/capability state exposed; failover only after anchor/checkpoint checks; no silent divergence. |
| API/telemetry | OpenAPI contract tests cover endpoints, keyset pagination limits, errors, liveness/readiness; metrics exist with bounded labels; logs/traces redact secrets. |
| Benchmark | Script, raw output, environment/fixture/indexes/concurrency/RPC p50-p95/CPU/RAM, throughput, checksum/row count, and duplicate count committed under `docs/benchmarks/`; a wrong checksum fails the validity gate. |
| Supply chain/demo | Clean checkout demo succeeds with local Anvil/PostgreSQL; format, vet, unit/integration/race/fuzz, security/secret scans, container build, SBOM/provenance validation, and negative evidence-validator tests pass in `make verify` and CI. |

Release requires all gates green, current architecture/threat model/ADRs/runbook/failure-mode docs, at least one visible negative demo, and README claims linked to actual evidence. No performance, audit, or production readiness claim is permitted before its evidence exists.

## Crash recovery and reorg boundary

The current executable performs one HTTP catch-up pass to the observed head and exits. It stores a trusted anchor, complete headers and filtered log-set fingerprints, and an ordered checkpoint. A changed checkpoint hash or parent triggers bounded parent-hash ancestor search. The replacement branch is fetched by immutable block hash for filtered logs, validated, then switched in one PostgreSQL transaction. Orphaned headers/logs remain stored; exact-match historical branches can become canonical again. A reorg at exactly configured max depth succeeds; a deeper or unprovable fork does not advance the checkpoint. Confirmed depth, missing-local-ancestor, corrupt-chain, identity, and provider-consistency failures persist an unsafe readiness reason; interrupted/transient fetch or DB work halts this process without changing the checkpoint. Recovery from a durable unsafe marker requires operator review and is not automatic.

A real-binary crash harness pauses at deterministic RPC, validation, append, reorg, and post-commit boundaries using hooks compiled only with the `crashprobe` build tag, sends SIGKILL, then restarts against the same PostgreSQL schema. A separate uninterrupted execution supplies the reference canonical checksum and row counts. SIGTERM cancels work and has a five-second shutdown deadline; a temporary PostgreSQL outage yields an unhealthy readiness result and bounded process failure, then safely resumes after recovery. The CLI exposes one address filter; the internal RPC filter supports bounded address/topic sets.

## Live synchronization boundary

Explicit `REORGGUARD_SYNC_MODE=live` runs startup HTTP catch-up, then one serial HTTP canonical sweep on coalesced WS head/log hints, every reconnect, and each configured polling interval. Heads cover empty blocks; logs (including `removed=true`) are hints only. A capacity-one wake queue bounds event bursts. The WS connection verifies chain ID and genesis; an outage does not prevent HTTP polling. Each sweep has a deadline, and reconnect uses capped exponential backoff with jitter. Live readiness distinguishes caught up, bounded lag, WS degradation with healthy HTTP, HTTP unavailability, and unsafe canonicality. The service mounts this status and its fixed-series metrics on an explicitly configured loopback API.

## RPC failover and concurrent fetch boundary

Checkpoint immutable header and filtered-log-set identity are compared to hash-bound RPC observations. A height-based query returning a different block hash during verification is branch movement; it enters bounded retry or reorg proof and is not classified as immutable identity corruption of the old hash. Every returned range log explicitly naming the stored checkpoint hash must match a stored immutable row, including all payload fields; a changed or unknown row is an identity error before movement classification. Logs naming a different hash are not compared to the old payload. An empty height-bound result alone cannot establish which branch was observed; complete log-set identity remains checked by hash.

The optional HTTP pool identifies each candidate by chain ID, configured genesis and non-genesis start anchor, durable checkpoint block hash/immutable header and filtered-log set, head height and the required range/block-hash `eth_getLogs` operations. An endpoint must not lag behind the checkpoint. A source proposing a different hash at the durable height must still reproduce the complete old checkpoint by hash and pass bounded common-ancestor proof before canonical switching. This also applies after restart, when no in-memory active provider exists. A mismatch evaluated against an old checkpoint expires when the durable checkpoint moves; wrong chain/genesis/anchor and missing required method remain process-lifetime hard rejects. Transient failures use finite cooldown and later reprobe. A provider that fails canonical work is excluded for the rest of that sweep. Among eligible endpoints, highest reported head wins, with configured order as a tie-break. This selection is an availability policy, not a consensus vote. If no safe candidate can progress, readiness is unhealthy. WS hints still route through this HTTP pool and the original canonical writer.

Parallel historical fetching is opt-in. At most the configured worker count and in-flight range count fetch headers/logs from the selected provider. Results can complete out of order, but only the next contiguous range can enter `Store.Append`; no later result advances the checkpoint across a gap. Failed waves discard uncommitted later results. Provider-specific adaptive windows persist across sweeps, while the durable checkpoint and reorg state are never reset during failover. Real-binary SIGKILL cases cover provider switch, in-flight fetch, buffered out-of-order result and open append transaction; restart from the same database matches an uninterrupted reference checksum. `api/openapi.yaml` defines an explicitly loopback-only live API. Log pages are canonical-only and use a read-only repeatable-read PostgreSQL transaction; a reorg changes the durable canonical revision and invalidates old cursors with HTTP 409. `sync_state` stores the last reorg summary and checkpoint update timestamp. Prometheus metrics, local stdout OTel tracing, structured logs and the full local Anvil demonstration are implemented. The deterministic 1,024-block sparse/dense/mixed benchmark has a SQL correctness gate; release checks include a scratch image, scanners, SBOM, unsigned build metadata, and clean-clone verification. External operating evidence and professional audit remain unavailable.
