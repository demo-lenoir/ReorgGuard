#!/bin/sh
set -eu

make verify-phase5
./scripts/scan-secrets.sh

if [ -n "$(gofmt -l cmd internal migrations)" ]; then
    echo 'gofmt is not clean' >&2
    exit 1
fi
go vet ./...
go test ./...
go test -race ./...
go test ./internal/model -run '^$' -fuzz=FuzzBuildRange -fuzztime=2s

scratch=$(mktemp -d)
container_id=''
cleanup() {
    if [ -n "$container_id" ]; then docker stop "$container_id" >/dev/null 2>&1 || true; fi
    rm -rf "$scratch"
}
trap cleanup EXIT INT TERM
container_id=$(docker run -d --rm -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 postgres:18.6)
ready=0
for _ in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -U postgres -d postgres >/dev/null 2>&1; then ready=1; break; fi
    sleep 0.25
done
if [ "$ready" -ne 1 ]; then echo 'PostgreSQL API container did not become ready' >&2; exit 1; fi
host_port=$(docker port "$container_id" 5432/tcp)
export REORGGUARD_TEST_DATABASE_URL="postgres://postgres@$host_port/postgres?sslmode=disable"
go test -race -tags=integration -run '^TestOperationalPage' -count=1 ./internal/store
docker stop "$container_id" >/dev/null
container_id=''

before=$(docker ps --format '{{.ID}}' | sort)
for run_number in 1 2 3; do
    echo "Anvil demo run $run_number/3"
    python3 scripts/demo.py | tee "$scratch/demo-$run_number.txt"
    grep -q '"result": "PASS"' "$scratch/demo-$run_number.txt"
done
after=$(docker ps --format '{{.ID}}' | sort)
if [ "$before" != "$after" ]; then echo 'demo left a Docker container running' >&2; exit 1; fi
echo 'Phase 6 verification passed.'
