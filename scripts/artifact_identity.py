"""Deterministic source and executable identity for local release evidence."""
import hashlib
import os
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def canonical_target(system, machine):
    """Map host architecture spellings to the Go executable target namespace."""
    goos = system.lower()
    aliases = {'x86_64': 'amd64', 'amd64': 'amd64',
               'aarch64': 'arm64', 'arm64': 'arm64'}
    goarch = aliases.get(machine.lower())
    if goos not in ('linux', 'darwin') or goarch is None:
        raise ValueError(f'unsupported executable target: {system}/{machine}')
    return f'{goos}/{goarch}'


def source_files(commit=None, docker=False):
    if commit:
        names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', commit], cwd=ROOT, text=True).splitlines()
    else:
        names = subprocess.check_output(['git', 'ls-files'], cwd=ROOT, text=True).splitlines()
    prefixes = ('cmd/', 'internal/', 'migrations/')
    exact = {'go.mod', 'go.sum'}
    if docker:
        exact.update({'Dockerfile', '.dockerignore'})
    return sorted(name for name in names if name.startswith(prefixes) or name in exact)


def source_digest(commit=None, docker=False):
    digest = hashlib.sha256()
    for name in source_files(commit, docker):
        if commit:
            data = subprocess.check_output(['git', 'show', f'{commit}:{name}'], cwd=ROOT)
        else:
            data = (ROOT / name).read_bytes()
        digest.update(name.encode() + b'\0' + len(data).to_bytes(8, 'big') + data)
    return digest.hexdigest()


def executable_digest(target, commit=None):
    system, architecture = target.split('/')
    if system not in ('linux', 'darwin') or architecture not in ('arm64', 'amd64'):
        raise ValueError('unsupported executable target')
    if commit and source_digest(commit) != source_digest():
        raise ValueError('current application source differs from recorded commit')
    with tempfile.TemporaryDirectory(prefix='rg-artifact-') as tmp:
        binary = pathlib.Path(tmp) / 'reorgguard'
        subprocess.run(['go', 'build', '-buildvcs=false', '-trimpath', '-ldflags=-s -w',
                        '-o', str(binary), './cmd/reorgguard'], cwd=ROOT,
                       env={**os.environ, 'CGO_ENABLED': '0', 'GOOS': system,
                            'GOARCH': architecture}, check=True, stdout=subprocess.DEVNULL)
        return hashlib.sha256(binary.read_bytes()).hexdigest()
