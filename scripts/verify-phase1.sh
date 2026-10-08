#!/bin/sh
set -eu

make verify-phase0
go version
go mod verify
unformatted=$(gofmt -l $(rg --files -g '*.go'))
if [ -n "$unformatted" ]; then echo "unformatted Go files: $unformatted" >&2; exit 1; fi
echo 'gofmt clean; go vet ./...'
go vet ./...
echo 'go test -count=1 ./...'
go test -count=1 ./...

container_id=$(docker run -d --rm -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 postgres:18.6)
cleanup() { docker stop "$container_id" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

ready=0
for _ in $(seq 1 60); do
  if docker exec "$container_id" pg_isready -U postgres -d postgres >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.25
done
if [ "$ready" -ne 1 ]; then echo 'PostgreSQL test container did not become ready' >&2; exit 1; fi
docker exec "$container_id" postgres --version
host_port=$(docker port "$container_id" 5432/tcp)
export REORGGUARD_TEST_DATABASE_URL="postgres://postgres@$host_port/postgres?sslmode=disable"
echo 'go test -tags=integration -count=1 ./...'
go test -tags=integration -count=1 ./...
echo 'go test -race -tags=integration -count=1 ./...'
go test -race -tags=integration -count=1 ./...
echo 'Go fuzz smoke: canonical range and RPC quantity parser'
go test ./internal/model -run '^$' -fuzz '^FuzzBuildRange$' -fuzztime 2s
go test ./internal/rpc -run '^$' -fuzz '^FuzzParseQuantity$' -fuzztime 2s
echo 'Phase 1 verification passed. Later make verify gates remain pending.'
