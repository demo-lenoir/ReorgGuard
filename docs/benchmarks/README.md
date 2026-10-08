# Local deterministic benchmark

`make benchmark` runs the real ReorgGuard binary against a deterministic local HTTP JSON-RPC fixture and PostgreSQL 18.6. It writes all 11 measured runs to ignored `raw.json`. `python3 scripts/validate-benchmark.py docs/benchmarks/raw.json` validates that result against the current source commit. The 96-block `make benchmark-smoke` is a shorter correctness and concurrency check for routine CI.

## Fixed workload

- The fixture generates blocks 0–1024 with SHA-256-derived hashes and parent links. Block 0 is the anchor; heights 1–1024 are indexed. Timestamps advance one second per block.
- Sparse has 85 canonical logs, one every twelfth block, with 32-byte data. Dense has 3,072 logs, three per block, with 96-byte data. Mixed has 1,368 logs in a deterministic 0–3-per-block pattern, with 48-byte data.
- Every log has one 32-byte topic, a 20-byte address, and 32-byte transaction and block hashes. At heights divisible by 17 the fixture also repeats exact log identities. The expected SQL projection contains one copy.
- Sequential fetch uses one worker; parallel runs use four workers/eight in-flight ranges or eight workers/16 in-flight ranges. Initial/minimum/maximum adaptive windows are 64/1/1024 blocks, with growth after three healthy ranges and a 10,000-log range cap. The pgx pool cap is eight connections.
- Every run creates a fresh PostgreSQL database in a temporary `postgres:18.6` container. The harness records host CPU/RAM/OS, Docker storage and resource limits, Go version, PostgreSQL version, indexes, RPC service-time p50/p95, process CPU, peak RSS, worker settings and observed RPC overlap.

The measured wall clock covers binary startup, migrations and indexing, excluding PostgreSQL container creation and the post-run SQL inspection. CPU and RSS cover ReorgGuard only. RPC p50/p95 is the fixture's response service time, not public-provider latency. The main workload adds no artificial fixture delay. The CI smoke adds a fixed 3 ms delay and requires concurrent RPC overlap in the parallel run.

## Correctness and evidence identity

Before the binary starts, the harness derives an expected SHA-256 from ordered immutable block and log fields. After completion, a separate SQL query checks checkpoint height/hash, canonical block and log counts, parent continuity, zero duplicate canonical logs, no orphan rows in this append-only workload, and the exact projection checksum. A failed check prevents throughput output.

The raw artifact records a real source commit, source digest, benchmark harness digest, executable target/digest, dataset identities, configuration, distinct run IDs, integer nanosecond durations and observed counts. The validator rebuilds the declared target and derives displayed wall time, blocks/s, logs/s and DB rows/s using exact decimal round-half-even rules. Negative tests reject tampered identities, counts, checkpoints, checksums, durations, rates, missing environment/configuration, absent parallel overlap and duplicated repeat runs.

The benchmark is local evidence for this fixed workload and machine. It is not a production SLA or a measurement of public RPC performance. The unsigned artifact cannot authenticate an entirely regenerated false dataset.

## Local results

Run `make benchmark` on the machine whose performance matters. The generated JSON records its actual environment, source commit, each run, and observed rates. `make verify` regenerates and validates this evidence; results from another commit are rejected.
