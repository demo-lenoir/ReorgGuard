# Failure matrix

| Failure | Expected behavior | Permanent evidence |
| --- | --- | --- |
| Process killed mid-batch | No partial checkpoint; restart from durable state | `TestRealProcessCrashBoundaries`, `TestMultiRPCBinaryCrash` |
| WS lost, missed blocks | HTTP polling/reconnect closes gap | `TestLiveReconnectGapRecoveryAndRemoved`, `TestLiveBinaryReconnectGap`, `make demo` |
| Provider range too large | Shrink and retry same start | `TestShrinkRetriesSameFromAndGrowth`, `TestPoolMidRangeFailureAndAdaptivePerEndpoint` |
| Stale/behind provider | Ineligible until safe | `TestPoolStaleAndBehindCheckpoint` |
| Reorg depths 1/2/N | Proven ancestor and atomic branch switch | `TestReconcileDepthsAndMixedLogs`, `TestFindAncestorDepthAndParentProof`, `make demo` |
| A→B→A | Known immutable branch reactivated | `TestReconcileABATransactionAndRestart`, `TestLiveBinaryReorgReconnectABA`, `make demo` |
| PostgreSQL outage | Bounded failure, unready, durable restart | `TestPostgreSQLTemporaryOutage`; API `TestLivenessReadinessAndRedaction` |
| Corrupt parent chain | Fail closed | `TestFindAncestorRejectsPlausibleHeightsWithBadParents` |
| Duplicate delivery | No duplicate canonical row | `TestPostgresAppendReplayIdentityAndRestart`, `TestLiveHeadsDuplicateOutOfOrderBurst` |
| Wrong-chain RPC | Hard reject | `TestPoolHardRejectsBadFallback`, `TestCancellationAndWrongChain` |
| All RPC unavailable | No checkpoint move, readiness unhealthy | `TestPoolAllUnavailable`, `TestLiveBothTransportsUnavailableAndRecovery` |
| Checkpoint mismatch | Replacement provider rejected | `TestPoolHardRejectsBadFallback`, `TestPoolNoConsensusFromProviderCount` |
| Ordered parallel fetch failure | Discard later results; no gap | `TestParallelMiddleFailureNoGap`, `TestParallelDBFailureDiscardsLaterResults` |
| API cursor crosses reorg | HTTP 409, restart page traversal | `TestOperationalPageRevisionAndReorgHistory`, `TestLogsPaginationFiltersAndErrors` |
| API-selected log has oversized data | HTTP 413; durable log remains intact | `TestOperationalOversizedPayloadIsStoredButNotServed`, `TestRequestCancellationDBTimeoutAndMetrics` |
| Benchmark checksum/row count tampered | Evidence validator rejects the artifact; no throughput is accepted | `test_validate_benchmark.RawEvidenceTest`, `make benchmark-smoke` |
| Parallel fetch silently serialized | Smoke requires observed overlapping RPC requests | `make benchmark-smoke`, `TestParallel*` |
| Docker image runs with privilege/writable root requirement | Runtime gate fails | `scripts/verify-image.py` |
