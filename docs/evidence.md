# Engineering evidence

This ledger maps each material claim to a reproducible command and a permanent test or artifact. The release gate is `make verify`; it includes the phase gates, real PostgreSQL integration, race and fuzz smoke, deterministic local Anvil runs, benchmark validation, container checks, supply-chain scans and a fresh-clone replay. The commands below run entirely against local fixtures.

| Claim | Command and evidence | Limit |
| --- | --- | --- |
| Canonical blocks, logs and checkpoints commit together | `make verify-phase1`; PostgreSQL integration in `internal/store` and `internal/backfill` | PostgreSQL durability depends on its configured storage. |
| Bounded reorgs switch branches atomically and permit A→B→A | `make verify-phase2`; `TestReconcileABATransactionAndRestart`, `TestReconcileDepthsAndMixedLogs` | Forks beyond the configured depth or retained ancestry fail closed. |
| SIGKILL and restart preserve the committed projection | `make verify-phase3`; real subprocess tests in `internal/crashharness` compare SQL snapshots and ordered checksums to uninterrupted runs | These fixtures are local, deterministic executions. |
| WS loss and duplicate hints converge through HTTP | `make verify-phase4`; `TestLiveReconnectGapRecoveryAndRemoved`, `TestLiveBinaryReconnectGap` | WebSocket only accelerates notification. |
| Guarded RPC failover and ordered parallel fetch preserve checkpoint order | `make verify-phase5`; `TestPoolMidRangeFailureAndAdaptivePerEndpoint`, `TestParallelMiddleFailureNoGap`, `TestParallelDBFailureDiscardsLaterResults` | RPC providers are data sources, not a consensus oracle. |
| Read API, readiness and telemetry reflect committed state | `make verify-phase6`; API and PostgreSQL tests in `internal/operational`; [OpenAPI](../api/openapi.yaml) | The API binds to loopback and has no public authentication layer. |
| Local Anvil exercises the end-to-end failure story | `make demo`; [demo steps](demo.md) and its final SQL checksum | The emitter is deployed only to local Anvil. |
| Throughput is reported only after SQL correctness checks | `make benchmark`; [methodology](benchmarks/README.md), generated `docs/benchmarks/raw.json`, `python3 scripts/validate-benchmark.py docs/benchmarks/raw.json` | The local fixture and host do not establish a production SLA. |
| False benchmark/provenance artifacts are rejected | `python3 -m unittest discover -s scripts -p 'test*.py'`; negative evidence tests and platform-specific executable digests | Local provenance is unsigned. |
| The image is minimal and runs without root or a writable filesystem | `python3 scripts/verify-image.py`; `make verify-phase7` | This is a reference image and local runtime test. |
| A fresh checkout can run the complete gate | `make verify-clean-clone`; it runs nested verification, a separate demo, benchmark smoke and image build | Requires the documented local toolchain and container runtime. |

The [failure matrix](failure-matrix.md) maps individual faults to exact tests. The [architecture](architecture.md), [threat model](threat-model.md), [ADRs](adr/) and [runbook](runbook.md) describe the boundaries and operator response. The release gate generates and checks an SPDX SBOM and unsigned local build metadata. Both are bound to the current source revision and remain outside the committed snapshot.
