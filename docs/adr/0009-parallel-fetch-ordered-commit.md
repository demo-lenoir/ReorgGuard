# ADR-0009: Bounded parallel historical fetch with ordered commit

Status: accepted.

## Context

HTTP log/header retrieval can spend time waiting on a provider while PostgreSQL writes are short and ordered. Parallel retrieval is useful for long historical catch-up, but committing out of order would violate the contiguous checkpoint invariant.

## Decision

Parallelism is opt-in. A wave plans at most `MaxInFlight` adjacent ranges with the current per-endpoint adaptive window. At most `Workers` fetch tasks perform HTTP calls concurrently. Every task has a bounded range deadline. Results occupy a bounded channel/map; a single ordered consumer waits for each planned start height, validates its parent against the just committed frontier, and invokes the existing PostgreSQL `Append` transaction. No later task may commit while an earlier task waits or fails. On failure, the wave is canceled and joined, uncommitted results are discarded, and the failed start height is retried within existing attempt/deadline bounds. A provider result limit shrinks that endpoint's window before replanning. Successful preceding ranges remain committed and are read again from PostgreSQL after provider failover or process restart.

Workers and result buffer are owned by the sweep. Shutdown waits for workers; no goroutine is detached. `Pool.Sweep` prevents concurrent canonical sweeps. Reorg resolution remains single-writer and sequential. The serial path remains default so small live increments do not pay concurrency overhead.

## Consequences

The maximum number of fetch goroutines and in-memory range results is configured and checked at startup. A slow first range can stall later commits, intentionally applying backpressure. Results arriving out of order affect latency only. The PostgreSQL schema and transaction boundary do not change. Real process-kill tests cover in-flight fetch, buffered later result, and an uncommitted append transaction; race tests cover reverse/random completion, failure, cancellation and bounded gauges.
