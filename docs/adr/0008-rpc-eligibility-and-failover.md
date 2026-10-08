# ADR-0008: Guarded endpoint eligibility and failover

Status: accepted.

## Context

The live and historical paths need another HTTP source when a provider is unavailable, stale, rate-limited, or lacks a required method. Independent RPC endpoints are not a consensus protocol and may disagree or share a faulty upstream.

## Decision

The shared direct/pooled checkpoint validator binds immutable header and filtered-log comparisons to the stored checkpoint hash. A later range query is height-bound, so branch movement is interpreted after another height lookup; a new hash is a fork candidate and inconsistent moving observations receive a bounded retry. This corrects the prior checkpoint-time A→B race without permitting same-hash immutable changes. Every returned range log explicitly naming the stored checkpoint hash must match a stored immutable row, including all payload fields; a changed or unknown row is an identity error before movement classification. Logs naming a different hash are not compared to the old payload. An empty height-bound result alone cannot establish which branch was observed; complete log-set identity remains checked by hash.

One configured endpoint is pinned for a complete canonical sweep. Before selection, each endpoint is probed with a bounded timeout for the expected chain ID, genesis block by number/hash, configured non-genesis start anchor, reported head, durable checkpoint block by hash including immutable header and filtered-log-set fields, and both range and block-hash `eth_getLogs` capability when a checkpoint exists. An endpoint behind the durable checkpoint is ineligible. A different canonical hash at the checkpoint height is a candidate fork only when the endpoint reproduces the complete old checkpoint by hash; the existing bounded reorg engine must then prove a parent-linked common ancestor before any switch. A wrong chain/genesis/anchor or absent required method is hard rejected. A checkpoint-relative mismatch is rejected for that checkpoint identity and rechecked after checkpoint movement. Probe failures and rate limits use a finite cooldown; the next sweep may reprobe after recovery. No endpoint URL appears in status, metrics, or logs.

Among eligible endpoints, choose the highest reported head, breaking equal-head ties in configured order. This is a deterministic freshness policy. It does not establish that the chosen branch is globally canonical. A selected endpoint's apparent fork uses the same local reorg engine and safety depth as a one-provider fork. If the selected operation fails transiently, the pool rereads the PostgreSQL checkpoint and probes another endpoint, with at most the configured bounded number of switches in one sweep. It never transfers a volatile fetch result across providers or retries a failed endpoint within that sweep. Immutable hard rejects are not retried indefinitely in that process. An all-incompatible set is terminal unsafe; temporary unavailability permits a later poll, with unhealthy readiness until a safe sweep succeeds.

Endpoint health has explicit states: `recovering` during selection, `healthy` after a successful sweep, `rate_limited` after provider-limit response, `degraded` after malformed data, `stale` when its head is behind the checkpoint or selected head, `temporarily_unavailable` after timeout/transport error, and `incompatible` after hard identity/method failure. A successful reprobe and sweep clears a transient state. Counters and last latency/head are observational. No opaque score or vote is used.

## Consequences and residual risk

Switching can improve availability while retaining the durable checkpoint and audit history. A later provider that agrees at the checkpoint may still lie about newer blocks or omit filtered logs. Parent/hash and log-association checks catch structural inconsistency, but cannot prove completeness of a plausible `eth_getLogs` response or global chain finality. Operator choice of RPC sources remains a trust assumption. An endpoint whose old checkpoint block or filtered-log set cannot be reproduced remains ineligible. A provider that proves old checkpoint history by hash can propose a shallow fork; only bounded ancestor search and the ordinary atomic reorg transaction can activate it.

## Checkpoint-relative eligibility and restart

The former active-endpoint exception depended on process memory and prevented restart during a valid shallow fork. After a restart, a provider that reproduces the complete durable checkpoint by hash may propose a fork. Parent-hash ancestor proof, depth policy, and the atomic canonical writer are unchanged; endpoint count is never a vote.

Checkpoint-relative mismatch and changed checkpoint observations are scoped to the exact durable `(height, hash)` at which they were seen. A committed checkpoint movement clears that rejection and requires a fresh probe. Wrong chain ID, genesis, configured anchor, and missing required method remain process-lifetime hard rejects. A provider that fails canonical work cannot be selected again in the same bounded sweep, even if its cooldown expires during another probe or DB read; cooldown applies to later sweeps.
