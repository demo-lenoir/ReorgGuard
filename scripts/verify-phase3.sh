#!/bin/sh
set -eu

make verify-phase2
./scripts/scan-secrets.sh

scratch=$(mktemp -d)
container_id=''
cleanup() {
    if [ -n "$container_id" ]; then docker stop "$container_id" >/dev/null 2>&1 || true; fi
    rm -rf "$scratch"
}
trap cleanup EXIT INT TERM

go build -tags=crashprobe -o "$scratch/reorgguard-crashprobe" ./cmd/reorgguard
container_id=$(docker run -d --rm -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 postgres:18.6)
ready=0
for _ in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -U postgres -d postgres >/dev/null 2>&1; then ready=1; break; fi
    sleep 0.25
done
if [ "$ready" -ne 1 ]; then echo 'PostgreSQL crash-test container did not become ready' >&2; exit 1; fi
host_port=$(docker port "$container_id" 5432/tcp)
export REORGGUARD_TEST_DATABASE_URL="postgres://postgres@$host_port/postgres?sslmode=disable"
export REORGGUARD_TEST_PG_CONTAINER="$container_id"
export REORGGUARD_CRASH_BINARY="$scratch/reorgguard-crashprobe"

echo 'Real binary SIGKILL/restart and SIGTERM suite, three consecutive runs'
go test -tags=crash -run 'TestRealProcessCrashBoundaries|TestReorgProcessKillRepeat10|TestSIGTERM' -count=3 -v ./internal/crashharness
echo 'Temporary PostgreSQL outage/recovery'
go test -tags=crash -run '^TestPostgreSQLTemporaryOutage$' -count=1 -v ./internal/crashharness

echo 'Race detector on crash controller and representative race-instrumented binary'
go build -race -tags=crashprobe -o "$scratch/reorgguard-crashprobe-race" ./cmd/reorgguard
export REORGGUARD_CRASH_BINARY="$scratch/reorgguard-crashprobe-race"
go test -race -tags=crash -run 'TestRealProcessCrashBoundaries/(append_before_commit|reorg_orphan|aba_return)|TestSIGTERM' -count=1 ./internal/crashharness
echo 'Phase 3 verification passed. Full make verify remains intentionally pending.'
