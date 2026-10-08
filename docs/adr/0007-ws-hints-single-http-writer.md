# ADR-0007: WebSocket hints wake one HTTP canonical writer

Status: accepted.

## Decision

Long-running mode owns one HTTP sweep loop. One WebSocket connection subscribes to `newHeads` and matching `logs`; both notifications enter a capacity-one coalesced wake channel. Heads cover empty blocks; log hints, including `removed=true`, prompt an earlier check for a fork. Neither carries canonical authority. The sweep reads the current HTTP head and invokes the existing `Coordinator.RunTo` append/reorg path from the durable PostgreSQL checkpoint. A reconnect always wakes an HTTP sweep even if no notification was delivered. A periodic poll does the same while WS is healthy or unavailable.

The WS worker owns connection, heartbeat, and reconnect; the caller owns the poll/sweep loop and joins the worker on shutdown. A failed connection uses capped exponential backoff with jitter. Invalid chain ID/genesis on WS is terminal; malformed notifications close that connection and fall back to HTTP polling while reconnecting. Transient HTTP/DB failure makes live readiness unhealthy and is retried on a later bounded poll. Proven unsafe canonicality remains fail-closed. Production builds have no crash control knobs.

## Consequences

The channel cannot accumulate one job per event. A missed/coalesced hint costs latency only. All durable state continues through the append/reorg validation and transaction path. This adds a direct `gorilla/websocket` dependency for bounded message reads and explicit connection ownership. The default CLI `once` mode remains for batch regression and batch operation; `REORGGUARD_SYNC_MODE=live` activates the long-running mode.
