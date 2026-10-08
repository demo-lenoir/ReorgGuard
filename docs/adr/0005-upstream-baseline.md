# ADR-0005: Pinned upstream baseline

Status: accepted.

## Decision

Build with Go 1.27.1, go-ethereum 1.17.6 and pgx 5.11.0. Use PostgreSQL 18.6 for deterministic local tests and benchmarks. `go.mod` pins module versions and `go.sum` records their checksums. The local Anvil fixture pins its Solidity compiler. GitHub Actions use full commit SHAs.

## Verification

The [Go release history](https://go.dev/doc/devel/release), [go-ethereum release](https://github.com/ethereum/go-ethereum/releases/tag/v1.17.6), [PostgreSQL 18 documentation](https://www.postgresql.org/docs/18/release.html), [Geth subscription behavior](https://geth.ethereum.org/docs/interacting-with-geth/rpc/pubsub), and [GitHub Actions security guidance](https://docs.github.com/en/actions/reference/security/secure-use) are the upstream references. Exact local toolchain and dependency versions are inspected by `make verify`.
