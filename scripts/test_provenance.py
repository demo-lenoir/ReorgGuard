import copy
import pathlib
import unittest
from importlib.machinery import SourceFileLoader

provenance = SourceFileLoader('provenance_tested', str(pathlib.Path(__file__).with_name('provenance.py'))).load_module()
from artifact_identity import executable_digest

class ProvenanceReferenceTest(unittest.TestCase):
    def setUp(self):
        self.reference = {'format': 'reorgguard-local-build-v2', 'target': 'linux/arm64',
                          'targets': {'linux/arm64': {'binary_sha256': 'd'},
                                      'linux/amd64': {'binary_sha256': 'e'}},
                          'source_input_sha256': 'source',
                          'dockerfile_sha256': 'a', 'go_mod_sha256': 'b',
                          'go_sum_sha256': 'c', 'binary_sha256': 'd',
                          'image_user': '65532:65532', 'entrypoint': ['/reorgguard']}

    def test_same_artifact_passes(self):
        provenance.validate_reference(self.reference, copy.deepcopy(self.reference))

    def test_binary_and_privilege_mismatch_fail(self):
        for field, value in [('binary_sha256', 'changed'), ('image_user', '0:0'),
                             ('entrypoint', ['/bin/sh']), ('go_mod_sha256', 'changed')]:
            with self.subTest(field=field):
                mutated = dict(self.reference)
                mutated[field] = value
                with self.assertRaisesRegex(ValueError, field):
                    provenance.validate_reference(mutated, self.reference)

    def test_cross_platform_artifact_is_rejected(self):
        reference = dict(self.reference, targets={
            'linux/arm64': {'binary_sha256': 'arm-binary'},
            'linux/amd64': {'binary_sha256': 'amd-binary'},
        })
        arm = dict(reference, target='linux/arm64', binary_sha256='arm-binary')
        amd = dict(reference, target='linux/amd64', binary_sha256='amd-binary')
        provenance.validate_reference(arm, reference)
        provenance.validate_reference(amd, reference)
        with self.assertRaises(ValueError):
            provenance.validate_reference(dict(arm, target='linux/amd64'), reference)
        with self.assertRaises(ValueError):
            provenance.validate_reference(dict(amd, target='linux/arm64'), reference)

    def test_missing_platform_and_within_target_change_fail(self):
        reference = dict(self.reference, targets={'linux/arm64': {'binary_sha256': 'arm-binary'}})
        current = dict(reference, target='linux/amd64', binary_sha256='amd-binary')
        with self.assertRaisesRegex(ValueError, 'platform'):
            provenance.validate_reference(current, reference)
        current.update(target='linux/arm64', binary_sha256='changed')
        with self.assertRaisesRegex(ValueError, 'binary_sha256'):
            provenance.validate_reference(current, reference)

    def test_cross_compiled_artifacts_have_distinct_platform_identities(self):
        arm = executable_digest('linux/arm64')
        amd = executable_digest('linux/amd64')
        self.assertNotEqual(arm, amd)
        reference = dict(self.reference, targets={
            'linux/arm64': {'binary_sha256': arm},
            'linux/amd64': {'binary_sha256': amd},
        })
        provenance.validate_reference(dict(reference, target='linux/arm64', binary_sha256=arm), reference)
        provenance.validate_reference(dict(reference, target='linux/amd64', binary_sha256=amd), reference)
        with self.assertRaisesRegex(ValueError, 'binary_sha256'):
            provenance.validate_reference(dict(reference, target='linux/amd64', binary_sha256=arm), reference)
