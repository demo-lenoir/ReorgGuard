#!/bin/sh
set -eu

for file in LICENSE SPEC.md README.md Makefile api/openapi.yaml docs/architecture.md docs/threat-model.md docs/runbook.md docs/failure-matrix.md docs/evidence.md docs/adr/0001-one-chain-fixed-filter-and-anchor.md docs/adr/0002-canonical-log-visibility.md docs/adr/0003-atomic-reorg-and-ordered-writer.md docs/adr/0004-rpc-health-not-consensus.md docs/adr/0005-upstream-baseline.md .github/workflows/phase0.yml; do
  test -s "$file" || { echo "missing or empty: $file" >&2; exit 1; }
done
for id in I01 I02 I03 I04 I05 I06 I07 I08 I09 I10 I11 I12 I13 I14 I15; do
  grep -q "| $id |" SPEC.md || { echo "missing invariant $id" >&2; exit 1; }
done
grep -q 'implementation contract' README.md
grep -q 'Apache License' LICENSE
grep -q 'SPDX-License-Identifier: Apache-2.0' contracts/EventEmitter.sol
echo 'Phase 0 contract files and invariant markers verified. Runtime behavior is not verified.'
