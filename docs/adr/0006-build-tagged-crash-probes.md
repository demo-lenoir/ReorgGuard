# ADR-0006: Build-tagged crash probes for process durability tests

Status: accepted.

## Decision

The normal `internal/crashprobe` implementation is a no-op. Only a binary explicitly built with `-tags=crashprobe` can report a named execution boundary to a localhost test controller and wait there. Integration tests run that binary as a real subprocess against deterministic local HTTP RPC and PostgreSQL, send SIGKILL or SIGTERM at the reported boundary, and restart against the exact same schema. Production binaries have no environment-controlled fault behavior.

## Reason

Sleep-based timing cannot reliably hit the narrow interval between an SQL update and commit, or between commit and observing success. The build tag provides deterministic evidence without introducing an unsafe production runtime switch or a new dependency. HTTP response holds cover crashes inside actual RPC calls; probes cover internal transaction and acknowledgement boundaries.

## Consequences

The probe is part of test instrumentation and must never become an authority for canonicality. Probe calls propagate cancellation, so SIGTERM can roll back in-flight work. A control connection is bounded and local to the test process. `make verify-phase3` builds both tagged and race-instrumented tagged binaries; ordinary `go build` uses the no-op implementation. The harness independently reads SQL state and compares it with an uninterrupted reference, so reaching a probe alone is not treated as proof of recovery.
