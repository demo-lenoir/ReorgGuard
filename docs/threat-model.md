# Threat model

ReorgGuard indexes a configured EVM filter into PostgreSQL. It does not hold signing keys or send transactions. The Anvil demo uses only an ephemeral local account. The assets are the canonical block/log projection, checkpoint and filter identity, database availability, RPC/API credentials, read API availability, and release evidence.

Trust boundaries are operator configuration → process, external HTTP/WS RPC → validation, validator → PostgreSQL, loopback API client → read service, and source/dependencies/CI → executable artifact.

| Actor or failure | Control and fail behavior | Evidence | Residual risk |
| --- | --- | --- | --- |
| Stale, malicious or equivocating RPC | Expected chain ID and genesis/anchor; hash/parent/log checks; bounded reorg proof; guarded failover | Wrong-chain, checkpoint mismatch, stale provider and corrupt-parent tests | Providers may share a bad source; plausible omitted logs cannot be proven absent |
| Oversized or malformed RPC/WS response | Byte, log and range bounds; parser validation; finite WS queue; HTTP catch-up | Fuzz, oversize, flood and disconnect tests | Provider denial of service can stop progress |
| Provider limits and transport failures | Bounded attempts, deadlines, backoff and per-provider adaptive range | Range evolution, minimum-window and cancellation tests | New provider error forms may need a profile update |
| Deep or unprovable reorg | Parent-linked common ancestor within inclusive maximum depth; atomic switch; fail-closed readiness | Depth 1/2/N/N+1, missing ancestor and A→B→A tests | Operator decision is needed beyond depth or retention |
| Duplicate or changed immutable identity | PostgreSQL keys, FK and partial unique index; complete block/log-set comparison | Replay, changed payload/set and returning-branch tests | A plausible omission at initial completion remains possible |
| DB outage, process death or interrupted transaction | One ordered writer; locked checkpoint row; atomic append/reorg transaction; bounded I/O | Real SIGKILL/restart, rollback, concurrent fetch and DB outage tests | Damaged storage or restored snapshots need operator repair |
| URL, API key or local key disclosure | Config-only secrets; redacted URLs/errors; bounded endpoint IDs; secret-pattern scan | API/log/metric/trace redaction tests and `scripts/scan-secrets.sh` | Host and environment access are outside the service |
| Slow or expensive API client | Loopback bind, read-only queries, 100-row page maximum, keyset cursor, five-second context and socket write deadlines, 32-request cap | Pagination, stale cursor, blocked write and cancellation tests | Public exposure requires a separate network/authentication policy |
| Compromised dependency or build | Pinned modules/actions, vulnerability and secret scans, scratch non-root image, SPDX SBOM and unsigned provenance checks | `make verify-phase7` and negative evidence-validator tests | Scanners cannot prove absence of compromise; metadata is unsigned |

WebSocket messages, including `removed=true`, only wake HTTP reconciliation. Returned `eth_getLogs` entries are locally checked against the configured address and positional topics; a mixed response fails as a whole. This does not prove completeness. Checkpoint immutable header and completed log-set checks bind to the stored hash in both direct and pooled modes. Branch movement at the same height enters bounded reconciliation; same-hash changed identity is rejected.

Readiness becomes unhealthy when safe canonical progress cannot be established even if the process remains live. A checkpoint-relative RPC rejection is re-evaluated after checkpoint movement; wrong chain/genesis/anchor stays incompatible. The test-only `crashprobe` build tag has no active production control path.

The [failure matrix](failure-matrix.md), [evidence ledger](evidence.md) and [runbook](runbook.md) connect these controls to tests and operator action. No professional audit or production operating history is claimed.
