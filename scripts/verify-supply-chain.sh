#!/bin/sh
set -eu
command -v trivy >/dev/null
command -v syft >/dev/null
if command -v govulncheck >/dev/null; then
    vuln=$(command -v govulncheck)
else
    vuln="$(go env GOPATH)/bin/govulncheck"
fi
if [ ! -x "$vuln" ]; then echo 'govulncheck v1.8.0 is required' >&2; exit 1; fi
if command -v actionlint >/dev/null; then
    actionlint_bin=$(command -v actionlint)
else
    actionlint_bin="$(go env GOPATH)/bin/actionlint"
fi
if [ ! -x "$actionlint_bin" ]; then echo 'actionlint v1.7.12 is required' >&2; exit 1; fi
case $("$actionlint_bin" -version | head -n 1) in
    v1.7.12) ;;
    *) echo 'actionlint v1.7.12 is required' >&2; exit 1 ;;
esac
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT INT TERM
"$vuln" ./... > "$scratch/govulncheck.txt"
"$actionlint_bin" .github/workflows/*.yml
trivy fs --scanners vuln --severity HIGH,CRITICAL --exit-code 1 --quiet . > "$scratch/trivy-source.txt"
trivy image --scanners vuln --severity HIGH,CRITICAL --exit-code 1 --quiet reorgguard:phase7-local > "$scratch/trivy-image.txt"
./scripts/scan-secrets.sh
DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') syft reorgguard:phase7-local -o "spdx-json=$scratch/sbom.spdx.json" --quiet
python3 scripts/provenance.py "$scratch/sbom.spdx.json" --output "$scratch/provenance.json"
python3 - "$scratch/sbom.spdx.json" "$scratch/provenance.json" <<'PY'
import hashlib, json, pathlib, subprocess, sys
sbom = json.loads(pathlib.Path(sys.argv[1]).read_text())
provenance = json.loads(pathlib.Path(sys.argv[2]).read_text())
assert sbom['spdxVersion'] == 'SPDX-2.3' and len(sbom['packages']) >= 1
assert provenance['sbom_sha256'] == hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest()
assert provenance['signed'] is False and provenance['image_user'] == '65532:65532'
assert provenance['source_commit'] == subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
import sys
sys.path.insert(0, str(pathlib.Path('scripts').resolve()))
import provenance as validator
reference = validator.with_platform_references(provenance)
validator.validate_reference(provenance, reference)
PY
mkdir -p dist
cp "$scratch/sbom.spdx.json" dist/local-sbom.spdx.json
cp "$scratch/provenance.json" dist/local-provenance.json
echo 'PASS govulncheck, actionlint, Trivy source/image, secret scan, SPDX SBOM, local provenance'
