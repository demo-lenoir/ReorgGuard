# ADR-0003: One ordered canonical writer and atomic branch switch

Status: accepted.

## Decision

Workers may fetch bounded ranges concurrently, but one coordinator submits only contiguous, validated work to a single canonical writer. It prefetches a bounded replacement branch and proves an ancestor before opening a DB transaction. The transaction locks and checks `sync_state`, updates losing/winning canonical flags and immutable data, and advances checkpoint last. On error, it rolls back the entire branch switch. No RPC occurs in the transaction.

## Reason and consequences

Out-of-order writes and separate rollback/replay commits could expose a checkpoint that names incomplete history. A single switch transaction makes crash behavior binary. Reorg size is bounded by max depth and resource limits; large catch-up after the switch is committed in later contiguous batches. Tests must inject failures at the last operation before commit, at commit ambiguity, and on restart.
