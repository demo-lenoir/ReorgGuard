# ADR-0004: Guarded RPC failover is not consensus

Status: accepted.

## Decision

Endpoint health tracks transport failures, latency, chain ID, head freshness, and method/range capability. Wrong-chain endpoints are rejected. Automatic failover requires matching network anchor and validated checkpoint hash/history boundary. Endpoint disagreement does not elect a canonical chain by majority. If history cannot be proven within max depth, readiness fails closed and an operator decides recovery.

## Reason and consequences

Providers can be stale or share an upstream fault. Chain ID alone does not identify a particular history. This conservative policy may pause during legitimate reorgs/provider disagreement; it prevents silent state corruption. Tests must cover stale/wrong-chain/mismatched endpoints and recovery after a compatible endpoint returns.
