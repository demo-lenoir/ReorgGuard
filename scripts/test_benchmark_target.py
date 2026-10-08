"""The benchmark producer and validator share Go target normalization."""
import unittest
from artifact_identity import canonical_target
from benchmark import canonical_target as producer_target
from importlib.machinery import SourceFileLoader
from pathlib import Path

validator = SourceFileLoader('benchmark_target_validator', str(Path(__file__).with_name('validate-benchmark.py'))).load_module()


class BenchmarkTargetTest(unittest.TestCase):
    def test_host_aliases(self):
        for host, arch, expected in [
            ('Linux', 'x86_64', 'linux/amd64'),
            ('Linux', 'aarch64', 'linux/arm64'),
            ('Darwin', 'x86_64', 'darwin/amd64'),
            ('Darwin', 'arm64', 'darwin/arm64'),
            ('Linux', 'AMD64', 'linux/amd64'),
        ]:
            with self.subTest(host=host, arch=arch):
                self.assertEqual(canonical_target(host, arch), expected)

    def test_unsupported_architecture_fails(self):
        with self.assertRaisesRegex(ValueError, 'unsupported'):
            canonical_target('Linux', 'mips64')

    def test_producer_and_validator_use_one_target_helper(self):
        self.assertIs(producer_target, canonical_target)
        self.assertIs(validator.canonical_target, canonical_target)


if __name__ == '__main__':
    unittest.main()
