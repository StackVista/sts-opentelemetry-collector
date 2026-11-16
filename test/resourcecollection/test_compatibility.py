"""Exercise the compatibility gate against real Git trees without creating commits."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from check_compatibility import CONTRACT, check


class CompatibilityGateTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        self.run_git("init", "-q")
        self.schema = CONTRACT + "resourcecollection-crd.yaml"
        self.assessment = CONTRACT + "compatibility/change.md"
        self.fixture = CONTRACT + "testdata/selection.json"

    def run_git(self, *args, input=None, env=None):
        return subprocess.check_output(
            ["git", *args], cwd=self.repo, input=input, env=env,
        ).decode().strip()

    def tree(self, files):
        index = self.repo / "fixture-index"
        index.unlink(missing_ok=True)
        env = os.environ | {"GIT_INDEX_FILE": str(index)}
        for path, content in files.items():
            blob = self.run_git("hash-object", "-w", "--stdin", input=content.encode())
            self.run_git("update-index", "--add", "--cacheinfo",
                         f"100644,{blob},{path}", env=env)
        return self.run_git("write-tree", env=env) if files else self.run_git(
            "mktree", input=b"")

    def test_unrelated_change_does_not_require_evidence(self):
        before = self.tree({self.schema: "schema", "README.md": "old"})
        after = self.tree({self.schema: "schema", "README.md": "new"})
        self.assertEqual(check(self.repo, before, after), "ResourceCollection contract unchanged")

    def test_initial_contract_with_assessment_and_fixture_passes(self):
        before = self.tree({})
        after = self.tree({self.schema: "schema", self.assessment: "# Initial API",
                           self.fixture: "{}"})
        self.assertIn(self.assessment, check(self.repo, before, after))

    def test_schema_change_without_evidence_fails(self):
        before = self.tree({self.schema: "old"})
        after = self.tree({self.schema: "new"})
        with self.assertRaisesRegex(ValueError, "assessment.*regressions"):
            check(self.repo, before, after)

    def test_unchanged_evidence_is_insufficient(self):
        files = {self.schema: "old", self.assessment: "# Existing", self.fixture: "{}"}
        before = self.tree(files)
        after = self.tree(files | {self.schema: "new"})
        with self.assertRaisesRegex(ValueError, "assessment.*regressions"):
            check(self.repo, before, after)

    def test_assessment_without_regression_fails(self):
        before = self.tree({self.schema: "old"})
        after = self.tree({self.schema: "new", self.assessment: "# Change"})
        with self.assertRaisesRegex(ValueError, "regressions"):
            check(self.repo, before, after)

    def test_regression_without_assessment_fails(self):
        before = self.tree({self.schema: "old"})
        after = self.tree({self.schema: "new", self.fixture: "{}"})
        with self.assertRaisesRegex(ValueError, "assessment"):
            check(self.repo, before, after)

    def test_deleted_or_empty_evidence_is_insufficient(self):
        for evidence in [{}, {self.assessment: " \n", self.fixture: ""}]:
            with self.subTest(evidence=evidence):
                before = self.tree({self.schema: "old", self.assessment: "# Old",
                                    self.fixture: "{}"})
                after = self.tree({self.schema: "new"} | evidence)
                with self.assertRaisesRegex(ValueError, "assessment.*regressions"):
                    check(self.repo, before, after)

    def test_wire_change_requires_evidence(self):
        wire = CONTRACT + "types.go"
        before = self.tree({wire: "old"})
        after = self.tree({wire: "new"})
        with self.assertRaisesRegex(ValueError, "assessment.*regressions"):
            check(self.repo, before, after)

    def test_updated_wire_regression_and_assessment_pass(self):
        wire = CONTRACT + "types.go"
        test = CONTRACT + "types_test.go"
        before = self.tree({wire: "old", test: "old", self.assessment: "# Old"})
        after = self.tree({wire: "new", test: "new", self.assessment: "# New"})
        self.assertIn(test, check(self.repo, before, after))

    def test_local_worktree_includes_new_files(self):
        before = self.tree({})
        files = {self.schema: "schema", self.assessment: "# Initial", self.fixture: "{}"}
        for path, content in files.items():
            target = self.repo / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        self.assertIn(self.fixture, check(self.repo, before, worktree=True))
        (self.repo / self.assessment).unlink()
        with self.assertRaisesRegex(ValueError, "assessment"):
            check(self.repo, before, worktree=True)

    def test_unknown_baseline_fails(self):
        with self.assertRaises(subprocess.CalledProcessError):
            check(self.repo, "a" * 40)

    def test_malformed_baseline_fails(self):
        with self.assertRaisesRegex(ValueError, "full Git object ID"):
            check(self.repo, "--help")


if __name__ == "__main__":
    unittest.main()
