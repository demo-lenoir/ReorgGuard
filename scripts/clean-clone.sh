#!/bin/sh
set -eu
export PYTHONDONTWRITEBYTECODE=1
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT INT TERM
clone="$scratch/reorgguard"
git clone --quiet --no-hardlinks . "$clone"
cd "$clone"
git submodule update --init --recursive
start=$(date +%s)
REORGGUARD_CLEAN_CLONE=1 make verify
make demo
python3 scripts/benchmark.py --smoke
docker build -q -t reorgguard:phase7-local --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" .
status=$(git status --porcelain --untracked-files=all)
if [ -n "$status" ]; then
    echo 'clean clone verification left tracked/untracked changes' >&2
    printf '%s\n' "$status" >&2
    exit 1
fi
end=$(date +%s)
echo "PASS clean clone: verify, demo, benchmark smoke, container build in $((end-start))s"
