#!/usr/bin/env python3
"""Negative regressions for the release benchmark correctness gate."""
import unittest
from benchmark import checksum, dataset, expected_lines, validate


class BenchmarkValidatorTest(unittest.TestCase):
    def setUp(self):
        blocks, logs = dataset('mixed', 24)
        self.snapshot = dict(checkpoint=24, head_hash=blocks[-1]['hash'][2:],
                             blocks=25, logs=len(logs), duplicates=0,
                             parent_errors=0, orphan_blocks=0, orphan_logs=0,
                             checksum=checksum(expected_lines(blocks, logs)))

    def test_reference_passes(self):
        validate(self.snapshot, dict(self.snapshot))

    def test_every_invariant_rejects_mutation(self):
        for key in self.snapshot:
            with self.subTest(key=key):
                altered = dict(self.snapshot)
                altered[key] = 'bad' if isinstance(altered[key], str) else altered[key] + 1
                with self.assertRaisesRegex(ValueError, key):
                    validate(self.snapshot, altered)

    def test_reference_changes_with_log_payload(self):
        blocks, logs = dataset('dense', 4)
        original = checksum(expected_lines(blocks, logs))
        logs[0]['data'] = '0xbeef'
        self.assertNotEqual(original, checksum(expected_lines(blocks, logs)))


if __name__ == '__main__':
    unittest.main()
