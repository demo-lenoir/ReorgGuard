import copy
import json
import pathlib
import unittest
from importlib.machinery import SourceFileLoader

validator = SourceFileLoader('validator', str(pathlib.Path(__file__).with_name('validate-benchmark.py'))).load_module()

class RawEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.raw = json.loads((pathlib.Path(__file__).resolve().parents[1] / 'docs/benchmarks/raw.json').read_text())

    def test_valid_evidence_passes(self):
        self.assertTrue(validator.validate_raw(self.raw))

    def test_rejects_mutated_checksum(self):
        bad = copy.deepcopy(self.raw)
        bad['runs'][0]['checksum'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'checksum'):
            validator.validate_raw(bad)

    def test_rejects_missing_configuration(self):
        bad = copy.deepcopy(self.raw)
        bad['runs'].pop(0)
        with self.assertRaisesRegex(ValueError, 'matrix'):
            validator.validate_raw(bad)

    def test_rejects_false_source_revision(self):
        bad = copy.deepcopy(self.raw)
        bad['harness_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'script'):
            validator.validate_raw(bad)

    def test_rejects_false_duplicate_evidence(self):
        bad = copy.deepcopy(self.raw)
        bad['runs'][0]['duplicate_rpc_deliveries'] = 0
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            validator.validate_raw(bad)

if __name__ == '__main__':
    unittest.main()
