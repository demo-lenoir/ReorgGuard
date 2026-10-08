#!/bin/sh
set -eu
make verify-phase6
python3 scripts/benchmark.py --output docs/benchmarks/raw.json >/dev/null
python3 -m unittest discover -s scripts -p 'test*.py'
python3 scripts/validate-benchmark.py docs/benchmarks/raw.json
python3 scripts/benchmark.py --smoke
rev=$(git rev-parse HEAD)
docker build -q -t reorgguard:phase7-local --build-arg SOURCE_REVISION="$rev" .
python3 scripts/verify-image.py
./scripts/verify-supply-chain.sh
echo 'Phase 7 local technical gate passed.'
