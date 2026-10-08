#!/bin/sh
set -eu

# Phase 1's gate covers all Go packages, including Phase 2's real PostgreSQL
# transaction/restart tests and the integration race run.
make verify-phase1
echo 'Phase 2 ancestor-search fuzz smoke'
go test ./internal/reorg -run '^$' -fuzz '^FuzzFindAncestor$' -fuzztime 2s
echo 'Phase 2 verification passed. Full make verify remains intentionally pending.'
