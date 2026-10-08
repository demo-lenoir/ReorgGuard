#!/bin/sh
set -eu

make verify-phase3
./scripts/scan-secrets.sh

if [ -n "$(gofmt -l cmd internal migrations)" ]; then
    echo 'gofmt is not clean' >&2
    exit 1
fi
go vet ./...
go test ./...
go test -race ./...
go test -race ./internal/live ./internal/rpc -count=3
go test ./internal/live -run '^TestLiveReconnectGapRecoveryAndRemoved$' -count=10
go test ./internal/rpc -run '^$' -fuzz=FuzzParseWSNotification -fuzztime=2s

scratch=$(mktemp -d)
container_id=''
cleanup() {
    if [ -n "$container_id" ]; then docker stop "$container_id" >/dev/null 2>&1 || true; fi
    rm -rf "$scratch"
}
trap cleanup EXIT INT TERM

go build -tags=crashprobe -o "$scratch/reorgguard-live" ./cmd/reorgguard
container_id=$(docker run -d --rm -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 postgres:18.6)
ready=0
for _ in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -U postgres -d postgres >/dev/null 2>&1; then ready=1; break; fi
    sleep 0.25
done
if [ "$ready" -ne 1 ]; then echo 'PostgreSQL live-test container did not become ready' >&2; exit 1; fi
host_port=$(docker port "$container_id" 5432/tcp)
export REORGGUARD_TEST_DATABASE_URL="postgres://postgres@$host_port/postgres?sslmode=disable"
export REORGGUARD_CRASH_BINARY="$scratch/reorgguard-live"

go test -tags='crash live' -run '^TestLiveBinary' -count=3 -v ./internal/crashharness
go test -tags='crash live' -run '^TestLiveBinaryReconnectGap/0x3$' -count=10 ./internal/crashharness

go build -race -tags=crashprobe -o "$scratch/reorgguard-live-race" ./cmd/reorgguard
export REORGGUARD_CRASH_BINARY="$scratch/reorgguard-live-race"
go test -race -tags='crash live' -run '^TestLiveBinary' -count=1 ./internal/crashharness
echo 'Phase 4 verification passed. Full make verify remains intentionally pending.'
