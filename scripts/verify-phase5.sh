#!/bin/sh
set -eu

make verify-phase4
./scripts/scan-secrets.sh

if [ -n "$(gofmt -l cmd internal migrations)" ]; then
    echo 'gofmt is not clean' >&2
    exit 1
fi
go vet ./...
go test ./...
go test -race ./...
go test -race ./internal/backfill -run '^TestParallel' -count=3
go test ./internal/model -run '^$' -fuzz=FuzzBuildRange -fuzztime=2s

scratch=$(mktemp -d)
container_id=''
cleanup() {
    if [ -n "$container_id" ]; then docker stop "$container_id" >/dev/null 2>&1 || true; fi
    rm -rf "$scratch"
}
trap cleanup EXIT INT TERM

go build -tags=crashprobe -o "$scratch/reorgguard-multirpc" ./cmd/reorgguard
container_id=$(docker run -d --rm -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 postgres:18.6)
ready=0
for _ in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -U postgres -d postgres >/dev/null 2>&1; then ready=1; break; fi
    sleep 0.25
done
if [ "$ready" -ne 1 ]; then echo 'PostgreSQL multi-RPC container did not become ready' >&2; exit 1; fi
host_port=$(docker port "$container_id" 5432/tcp)
export REORGGUARD_TEST_DATABASE_URL="postgres://postgres@$host_port/postgres?sslmode=disable"
export REORGGUARD_CRASH_BINARY="$scratch/reorgguard-multirpc"

go test -tags=integration -run '^TestPool' -count=1 -v ./internal/multirpc
go test -race -tags=integration -run '^TestPool' -short -count=1 ./internal/multirpc
echo 'Ten consecutive primary-to-fallback exact-checkpoint continuations'
go test -tags=integration -run '^TestPoolPrimaryAndGuardedFailover/transport$' -count=10 ./internal/multirpc
echo 'Ten consecutive mid-backfill failovers with per-provider range shrink'
go test -tags=integration -run '^TestPoolMidRangeFailureAndAdaptivePerEndpoint$' -count=10 ./internal/multirpc
echo 'Ten consecutive reverse/random parallel completion runs'
go test -race ./internal/backfill -run '^TestParallelReverseAndRandomCompletion$' -count=10

echo 'Real binary SIGKILL/restart at failover, fetch, ordered wait and transaction boundaries'
go test -tags='crash live multirpc' -run '^TestMultiRPCBinaryCrash$' -count=3 -v ./internal/crashharness
go build -race -tags=crashprobe -o "$scratch/reorgguard-multirpc-race" ./cmd/reorgguard
export REORGGUARD_CRASH_BINARY="$scratch/reorgguard-multirpc-race"
go test -race -tags='crash live multirpc' -run '^TestMultiRPCBinaryCrash$' -count=1 ./internal/crashharness
echo 'Phase 5 verification passed. Full make verify remains intentionally pending.'
