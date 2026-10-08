#!/usr/bin/env python3
"""Unsigned, local build metadata. This is not a SLSA attestation."""
import argparse
import hashlib
import json
import pathlib
import subprocess
import tempfile
from artifact_identity import source_digest, executable_digest

ROOT = pathlib.Path(__file__).resolve().parents[1]

def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()

def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()

def binary_sha256(image):
    container = command('docker', 'create', image)
    try:
        with tempfile.TemporaryDirectory(prefix='rg-provenance-') as tmp:
            target = pathlib.Path(tmp) / 'reorgguard'
            subprocess.run(['docker', 'cp', f'{container}:/reorgguard', str(target)], check=True, stdout=subprocess.DEVNULL)
            return sha(target)
    finally:
        subprocess.run(['docker', 'rm', container], check=True, stdout=subprocess.DEVNULL)


def validate_reference(current, reference):
    if current.get('format') != 'reorgguard-local-build-v2' or reference.get('format') != 'reorgguard-local-build-v2':
        raise ValueError('unsupported provenance format')
    for field in ('dockerfile_sha256', 'go_mod_sha256', 'go_sum_sha256',
                  'source_input_sha256', 'image_user', 'entrypoint'):
        if current.get(field) != reference.get(field):
            raise ValueError(f'local build differs from committed reference: {field}')
    target = current.get('target')
    targets = reference.get('targets')
    if not isinstance(targets, dict) or target not in targets:
        raise ValueError(f'missing platform reference: {target}')
    expected = targets[target]
    if not isinstance(expected, dict) or current.get('binary_sha256') != expected.get('binary_sha256'):
        raise ValueError(f'local build differs from committed reference: binary_sha256 for {target}')


def create(sbom, image='reorgguard:phase7-local'):
    sbom_data = json.loads(pathlib.Path(sbom).read_text())
    if sbom_data.get('spdxVersion') != 'SPDX-2.3' or not sbom_data.get('packages'):
        raise ValueError('invalid or empty SPDX SBOM')
    inspect = json.loads(command('docker', 'image', 'inspect', image))[0]
    config = inspect['Config']
    if config['User'] != '65532:65532' or config['Entrypoint'] != ['/reorgguard']:
        raise ValueError('image privilege or entrypoint mismatch')
    if any('KEY=' in x or 'PASSWORD=' in x or 'TOKEN=' in x for x in config.get('Env', [])):
        raise ValueError('credential-like image environment')
    target = f"{inspect['Os']}/{inspect['Architecture']}"
    if target not in ('linux/arm64', 'linux/amd64'):
        raise ValueError(f'unsupported image platform: {target}')
    return {'format': 'reorgguard-local-build-v2', 'source_commit': command('git', 'rev-parse', 'HEAD'),
            'dockerfile_sha256': sha(ROOT / 'Dockerfile'), 'go_mod_sha256': sha(ROOT / 'go.mod'),
            'go_sum_sha256': sha(ROOT / 'go.sum'), 'sbom_sha256': sha(sbom),
            'source_input_sha256': source_digest(docker=True),
            'image_id': inspect['Id'], 'target': target,
            'binary_sha256': binary_sha256(image),
            'image_user': config['User'],
            'entrypoint': config['Entrypoint'], 'signed': False}


def with_platform_references(current):
    targets = {target: {'binary_sha256': executable_digest(target)}
               for target in ('linux/arm64', 'linux/amd64')}
    if current['binary_sha256'] != targets[current['target']]['binary_sha256']:
        raise ValueError('container executable differs from reproducible declared target build')
    return {**current, 'targets': targets}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sbom')
    parser.add_argument('--output')
    parser.add_argument('--reference-output')
    args = parser.parse_args()
    data = create(args.sbom)
    if args.reference_output:
        data = with_platform_references(data)
    encoded = json.dumps(data, indent=2, sort_keys=True) + '\n'
    if args.reference_output or args.output:
        pathlib.Path(args.reference_output or args.output).write_text(encoded)
    else:
        print(encoded)
