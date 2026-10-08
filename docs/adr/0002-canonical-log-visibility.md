# ADR-0002: Preserve immutable logs; derive canonical visibility

Status: accepted.

## Decision

Keep seen block and filtered log identity/payload immutable. Blocks have a mutable canonical flag, with a unique partial index on `(chain_id, number) WHERE canonical`. Canonical log reads join to canonical blocks. Complete per-block filtered log-set identity is stored/checked. A WebSocket log with `removed=true` is a reconciliation hint; it does not directly mutate or delete a log. A returning A→B→A branch reuses matching rows.

## Reason and consequences

A mutable `removed` field cannot by itself prove a returning branch has exactly the prior payload/log set. This ADR retains immutable branch identity and derives visibility from canonical blocks. PostgreSQL partial unique indexes are documented at <https://www.postgresql.org/docs/18/sql-createindex.html>. Geth documents `removed=true` and repeated log delivery at <https://geth.ethereum.org/docs/interacting-with-geth/rpc/pubsub>. Tests must cover exact reactivation and changed identity rejection.
