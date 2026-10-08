#!/usr/bin/env python3
"""Reject benchmark evidence that does not match the deterministic fixture."""
import argparse
import hashlib
import json
import math
import pathlib
import subprocess
from decimal import Decimal, ROUND_HALF_EVEN
from benchmark import checksum, dataset, expected_lines, run_identity, dataset_identity
from artifact_identity import source_digest, executable_digest, canonical_target

ROOT = pathlib.Path(__file__).resolve().parents[1]


def validate_raw(raw, recorded_script=None):
    if raw.get('harness_version') != 3 or not isinstance(raw.get('height'), int) or raw['height'] < 64:
        raise ValueError('invalid harness version or height')
    source = raw.get('source_commit', '')
    if len(source) != 40 or any(c not in '0123456789abcdef' for c in source):
        raise ValueError('invalid source commit')
    if recorded_script is None:
        try:
            recorded_script = subprocess.check_output(
                ['git', 'show', f'{source}:scripts/benchmark.py'], cwd=ROOT,
                stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as error:
            raise ValueError('unknown source commit') from error
    recorded_hash = hashlib.sha256(recorded_script).hexdigest()
    if raw.get('harness_sha256') != recorded_hash:
        raise ValueError('benchmark script differs from recorded commit')
    if hashlib.sha256((ROOT / 'scripts/benchmark.py').read_bytes()).hexdigest() != recorded_hash:
        raise ValueError('current benchmark harness differs from recorded commit')
    expected_source = source_digest(source)
    if raw.get('app_source_sha256') != expected_source or source_digest() != expected_source:
        raise ValueError('benchmark application source differs from recorded commit')
    environment = raw.get('environment')
    if not isinstance(environment, dict):
        raise ValueError('missing benchmark environment')
    for field in ('os', 'arch', 'cpu', 'go', 'postgres_image', 'postgres_version', 'storage', 'indexes', 'container_limits'):
        if not environment.get(field):
            raise ValueError(f'missing benchmark environment {field}')
    if 'go1.27.1' not in environment['go'] or not str(environment['postgres_version']).startswith('18.6'):
        raise ValueError('incorrect benchmark toolchain or PostgreSQL version')
    if not isinstance(environment['indexes'], list) or len(environment['indexes']) < 5:
        raise ValueError('missing benchmark indexes')
    if not isinstance(environment['container_limits'], dict) or environment['container_limits'].get('logical_cpus', 0) < 1:
        raise ValueError('invalid benchmark container limits')
    target = canonical_target(environment['os'], environment['arch'])
    if raw.get('binary_target') != target:
        raise ValueError('benchmark binary platform differs from environment')
    if raw.get('dataset_version') != 'reorgguard-benchmark-v1':
        raise ValueError('invalid benchmark dataset version')
    config = raw.get('config')
    expected_config = {'initial_range': 64, 'min_range': 1, 'max_range': 1024,
                       'db_pool_max_conns': 8, 'rpc_fixture_delay_ms': 0,
                       'adaptive_growth_after': 3, 'max_logs_per_range': 10000}
    if config != expected_config:
        raise ValueError('invalid benchmark configuration')
    runs = raw.get('runs')
    required = [(scenario, workers) for scenario in ('sparse', 'dense', 'mixed')
                for workers in (1, 4, 8)] + [('mixed', 4), ('mixed', 4)]
    if not isinstance(runs, list) or len(runs) != len(required):
        raise ValueError('required configuration/repeatability matrix missing')
    seen_runs = set()
    for index, (run, (wanted_scenario, wanted_workers)) in enumerate(zip(runs, required)):
        scenario = run.get('scenario')
        workers = run.get('workers')
        if (scenario, workers) != (wanted_scenario, wanted_workers):
            raise ValueError('incorrect scenario/worker plan')
        identity = run_identity(source, index, scenario, workers, raw['height'])
        if run.get('run_index') != index or run.get('run_id') != identity or identity in seen_runs:
            raise ValueError('invalid or repeated run identity')
        seen_runs.add(identity)
        if run.get('max_in_flight') != (0 if workers == 1 else workers * 2):
            raise ValueError('incorrect in-flight configuration')
        blocks, logs = dataset(scenario, raw['height'])
        reference = checksum(expected_lines(blocks, logs))
        if run.get('dataset_sha256') != dataset_identity(scenario, raw['height'], reference):
            raise ValueError('incorrect dataset identity')
        if run.get('blocks_processed') != raw['height'] or run.get('logs_processed') != len(logs):
            raise ValueError('incorrect processed block/log counts')
        for field, wanted in [('checkpoint', raw['height']), ('canonical_blocks', raw['height'] + 1),
                              ('canonical_logs', len(logs)), ('duplicate_canonical_logs', 0), ('parent_errors', 0),
                              ('orphan_blocks', 0), ('orphan_logs', 0),
                              ('checksum', reference), ('reference_checksum', reference), ('correctness', 'PASS')]:
            if run.get(field) != wanted:
                raise ValueError(f'invalid benchmark {field}')
        for field in ('wall_seconds', 'blocks_per_second', 'db_rows_per_second', 'rpc_latency_p50_ms',
                      'rpc_latency_p95_ms', 'peak_process_rss_bytes', 'logs_per_second'):
            value = run.get(field)
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f'invalid benchmark {field}')
        nanoseconds = run.get('wall_nanoseconds')
        if type(nanoseconds) is not int or nanoseconds <= 0:
            raise ValueError('invalid integer benchmark duration')
        precise_seconds = Decimal(nanoseconds) / Decimal(1000000000)
        displayed_seconds = precise_seconds.quantize(Decimal('0.0001'), rounding=ROUND_HALF_EVEN)
        if displayed_seconds <= 0 or Decimal(str(run['wall_seconds'])) != displayed_seconds:
            raise ValueError('invalid benchmark wall_seconds arithmetic')
        for field, count in [('blocks_per_second', raw['height']), ('logs_per_second', len(logs)),
                             ('db_rows_per_second', raw['height'] + 1 + len(logs))]:
            expected_rate = (Decimal(count) * Decimal(1000000000) / Decimal(nanoseconds)).quantize(Decimal('0.01'), rounding=ROUND_HALF_EVEN)
            if Decimal(str(run[field])) != expected_rate:
                raise ValueError(f'invalid benchmark {field} arithmetic')
        if run['rpc_latency_p95_ms'] < run['rpc_latency_p50_ms'] or run.get('range_requests', 0) < 2:
            raise ValueError('invalid RPC range evidence')
        if run.get('duplicate_rpc_deliveries', 0) == 0:
            raise ValueError('duplicate fixture not exercised')
        if workers > 1 and run.get('max_concurrent_rpc', 0) < 2:
            raise ValueError('parallel run had no observed RPC overlap')
    binary = raw.get('binary_sha256')
    if not isinstance(binary, str) or len(binary) != 64 or any(c not in '0123456789abcdef' for c in binary):
        raise ValueError('invalid benchmark binary identity')
    if executable_digest(target, source) != binary:
        raise ValueError('benchmark executable differs from recorded source/platform')
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    args = parser.parse_args()
    validate_raw(json.loads(pathlib.Path(args.path).read_text()))
    print('PASS benchmark evidence validation')
