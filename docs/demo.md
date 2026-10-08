# Local Anvil demonstration

Run `make demo` from a fresh checkout. Prerequisites: Go 1.27.1, Docker with the daemon running, Foundry (`anvil`, `forge`, `cast`), Python 3, PostgreSQL `psql`, and `make`. Forge may download the pinned Solidity compiler on first use. The script builds both local Go binaries and the tiny event fixture contract, starts a temporary PostgreSQL 18.6 container, and starts Anvil and two loopback-only transport fault proxies. It supplies ephemeral local configuration; `.env` is unnecessary. Genesis and every mined block use explicit timestamps, making the canonical checksum repeatable for the same toolchain.

The automatic flow is:

1. Deploy the local `EventEmitter` fixture, emit a historical event, start ReorgGuard, and wait for its durable checkpoint and readiness.
2. Emit another event with WS connected and verify live indexing.
3. Disable WS and both HTTP proxies, emit an event, verify readiness is unhealthy and the checkpoint has not moved. Restore HTTP while WS remains down, verify polling fills the missing height and readiness recovers, then restore WS and wait for reconnection.
4. Snapshot Anvil, emit branch A, then revert the snapshot and emit branch B at the same height. Verify A's event is orphaned and B is canonical. Revert again and replay A with the same timestamp and transaction so its original hash returns; verify exact A→B→A re-canonicalization without a primary-key collision.
5. SIGKILL ReorgGuard, restart the same binary against the same database, and verify its durable checkpoint.
6. Disable primary HTTP, emit another event, and verify guarded `fallback_1` continuation.
7. Query `/v1/status`, `/v1/logs`, `/health/ready`, and `/metrics`. Independently query PostgreSQL for checkpoint identity, canonical/orphan counts, log ownership, duplicate identities and a SHA-256 canonical projection ordered over immutable block and log fields. Print a final PASS summary.

Anvil's `evm_revert` removes the old block from its hash lookup. The demo proxy preserves the exact old header and filtered logs observed **before** each revert, then serves them only for old-hash lookups. This models an archival endpoint and lets the checkpoint eligibility check remain unchanged. The replacement branch still comes from Anvil over HTTP and passes normal parent/log validation. The proxy is test infrastructure, never part of ReorgGuard production runtime.

All HTTP listeners bind to `127.0.0.1`. The script terminates its child processes and removes its temporary container on success or failure. It does not deploy to a public network or use a private key; transactions are sent from Anvil's unlocked local account. A normal run takes a few seconds after toolchain installation. The release gate runs the demo three times and checks Docker cleanup.
