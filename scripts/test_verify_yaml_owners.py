#!/usr/bin/env python3
"""Negative controls use temporary Git trees; no fixture commits or checkout edits."""
import importlib.util
import io
import json
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("owners", Path(__file__).with_name("verify-yaml-owners.py"))
owners = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owners)
ROOT = Path(__file__).resolve().parents[1]


class CommittedOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        shutil.copytree(ROOT / "third_party", self.root / "third_party")
        self.git("init", "-q")
        self.git("add", "-f", "third_party")
        self.tree = self.git("write-tree").decode().strip()

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.root), *args], stderr=subprocess.DEVNULL)

    def expect_failure(self, message):
        with self.assertRaisesRegex(ValueError, message):
            owners.verify(self.root, self.git("write-tree").decode().strip())

    def archive_edit(self, transform):
        path = self.root / "third_party/uap-go/uap-core/UPSTREAM-GITHUB.tar"
        with tarfile.open(path) as archive:
            members = [(m, archive.extractfile(m).read()) for m in archive.getmembers() if m.isfile()]
        with tarfile.open(path, "w") as archive:
            for member, data in transform(members):
                archive.addfile(member, io.BytesIO(data))
        self.git("add", str(path.relative_to(self.root)))

    def test_complete_committed_sources_pass(self):
        owners.verify(self.root, self.tree)

    def test_ignored_disk_asset_cannot_hide_git_omission(self):
        path = "third_party/uap-go/uap-core/regexes.yaml"
        self.git("update-index", "--force-remove", path)
        (self.root / ".gitignore").write_text(path + "\n")
        self.assertTrue((self.root / path).is_file())
        self.expect_failure("worktree differs")
        # Even if the worktree comparison is bypassed, Git inventory is incomplete.
        _, files = owners.committed_files(self.root, self.git("write-tree").decode().strip())
        inventory = json.loads(files["third_party/yaml-sources.json"][0])
        prefix = "third_party/uap-go/"
        with self.assertRaisesRegex(ValueError, "missing or added"):
            owners.verify_owner("uap-go", inventory["uap-go"],
                                {p[len(prefix):]: v for p, v in files.items() if p.startswith(prefix)})

    def test_missing_archived_asset(self):
        self.archive_edit(lambda members: [])
        self.expect_failure("missing or added")

    def test_archived_executable_mode_change(self):
        def change(members):
            members[0][0].mode |= 0o111
            return members
        self.archive_edit(change)
        self.expect_failure("changed Git mode")

    def test_committed_source_edit(self):
        path = self.root / "third_party/uap-go/uaparser/parser.go"
        path.write_bytes(path.read_bytes() + b"\n// unrelated behavior edit\n")
        self.git("add", str(path.relative_to(self.root)))
        self.expect_failure("changed source/asset")

    def test_attributed_non_parser_edit_still_fails(self):
        import hashlib
        path = self.root / "third_party/uap-go/uaparser/parser.go"
        path.write_bytes(path.read_bytes() + b"\n// unrelated behavior edit\n")
        inventory = self.root / "third_party/yaml-sources.json"
        data = json.loads(inventory.read_text())
        data["uap-go"]["patched_files"]["uaparser/parser.go"] = hashlib.sha256(path.read_bytes()).hexdigest()
        inventory.write_text(json.dumps(data))
        self.git("add", "third_party")
        self.expect_failure("non-parser source patch")

    def test_unexpected_committed_asset(self):
        path = self.root / "third_party/uap-go/unexpected.yaml"
        path.write_text("extra: true\n")
        self.git("add", "-f", str(path.relative_to(self.root)))
        self.expect_failure("missing or added")

    def test_missing_committed_license(self):
        self.git("rm", "-q", "-f", "third_party/scaleway-sdk-go/LICENSE")
        self.expect_failure("missing or added")

    def test_changed_committed_license(self):
        path = self.root / "third_party/scaleway-sdk-go/LICENSE"
        path.write_text("replacement license\n")
        self.git("add", str(path.relative_to(self.root)))
        self.expect_failure("changed source/asset")

    def test_original_executable_mode_preserved(self):
        path = self.root / "third_party/uap-go/build.sh"
        path.chmod(0o644)
        self.git("add", str(path.relative_to(self.root)))
        self.expect_failure("changed Git mode")

    def test_index_only_mode_change(self):
        self.git("update-index", "--chmod=-x", "third_party/uap-go/build.sh")
        self.expect_failure("worktree differs")

    def test_untracked_local_asset(self):
        (self.root / "third_party/uap-go/extra.yaml").write_text("extra: true\n")
        with self.assertRaisesRegex(ValueError, "worktree differs"):
            owners.verify(self.root, self.tree)


if __name__ == "__main__":
    unittest.main()
