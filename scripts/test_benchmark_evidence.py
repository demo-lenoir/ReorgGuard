"""Mutations that must never validate as measured release evidence."""
import copy
import json
import pathlib
import subprocess
import unittest
from importlib.machinery import SourceFileLoader
from benchmark import rounded_measurement

ROOT = pathlib.Path(__file__).resolve().parents[1]
validator = SourceFileLoader('benchmark_evidence_validator', str(ROOT / 'scripts/validate-benchmark.py')).load_module()


class BenchmarkArtifactMutationTest(unittest.TestCase):
    def setUp(self):
        self.raw = json.loads((ROOT / 'docs/benchmarks/raw.json').read_text())

    def test_false_measurements_and_metadata_fail(self):
        mutations = {
            'invented_throughput': lambda r: r['runs'][0].update(blocks_per_second=1e15),
            'missing_environment': lambda r: r.pop('environment'),
            'missing_configuration': lambda r: r.pop('config'),
            'worker_relabel': lambda r: r['runs'][1].update(workers=8),
            'no_parallel_overlap': lambda r: r['runs'][1].update(max_concurrent_rpc=1),
            'cloned_repeats': lambda r: r['runs'].__setitem__(slice(-2, None), [copy.deepcopy(r['runs'][7]) for _ in range(2)]),
            'changed_source_commit': lambda r: r.update(source_commit='0' * 40),
            'changed_binary': lambda r: r.update(binary_sha256='0' * 64),
            'changed_platform': lambda r: r.update(binary_target='linux/amd64'),
            'changed_checksum': lambda r: r['runs'][0].update(checksum='0' * 64),
            'changed_checkpoint': lambda r: r['runs'][0].update(checkpoint=1),
            'changed_dataset': lambda r: r['runs'][0].update(dataset_sha256='0' * 64),
            'changed_duration': lambda r: r['runs'][0].update(wall_seconds=100),
            'changed_blocks': lambda r: r['runs'][0].update(blocks_processed=1),
            'changed_logs': lambda r: r['runs'][0].update(logs_processed=1),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                raw = copy.deepcopy(self.raw)
                mutate(raw)
                with self.assertRaises(ValueError):
                    validator.validate_raw(raw)

    def test_exact_duration_and_each_rate_are_bound(self):
        run = self.raw['runs'][0]
        mutations = {
            'tiny_duration_and_absurd_blocks': {'wall_seconds': 0.000050000000001, 'blocks_per_second': 1e15},
            'duration_only': {'wall_seconds': run['wall_seconds'] + 0.0001},
            'nanoseconds_only': {'wall_nanoseconds': run.get('wall_nanoseconds', 1) + 100000000},
            'blocks_rate_only': {'blocks_per_second': run['blocks_per_second'] + 0.01},
            'logs_rate_only': {'logs_per_second': run['logs_per_second'] + 0.01},
            'db_rows_rate_only': {'db_rows_per_second': run['db_rows_per_second'] + 0.01},
            'above_display_boundary': {'wall_seconds': run['wall_seconds'] + 0.000000000001},
            'below_display_boundary': {'wall_seconds': run['wall_seconds'] - 0.000000000001},
        }
        for name, change in mutations.items():
            with self.subTest(name=name):
                raw = copy.deepcopy(self.raw)
                raw['runs'][0].update(change)
                with self.assertRaises(ValueError):
                    validator.validate_raw(raw)

    def test_rounding_boundary_counterexamples(self):
        tiny = copy.deepcopy(self.raw)
        tiny['runs'][0].update(wall_nanoseconds=50000, wall_seconds=0.000050000000001,
                               blocks_per_second=1e15)
        with self.assertRaisesRegex(ValueError, 'wall_seconds'):
            validator.validate_raw(tiny)

        rounded_tiny = copy.deepcopy(self.raw)
        rounded_tiny['runs'][0].update(wall_nanoseconds=50001, wall_seconds=0.0001,
                                       blocks_per_second=1e15)
        with self.assertRaisesRegex(ValueError, 'blocks_per_second'):
            validator.validate_raw(rounded_tiny)

    def test_consistent_duration_change_without_rate_update_fails(self):
        changed = copy.deepcopy(self.raw)
        run = changed['runs'][0]
        run['wall_nanoseconds'] += 100000000
        run['wall_seconds'] = rounded_measurement(run['wall_nanoseconds'])
        with self.assertRaisesRegex(ValueError, 'blocks_per_second'):
            validator.validate_raw(changed)


if __name__ == '__main__':
    unittest.main()
