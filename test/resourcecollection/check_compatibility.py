#!/usr/bin/env python3
"""Require an assessment and regressions when the ResourceCollection contract changes."""

import argparse
from pathlib import Path
import re
import subprocess
import sys


CONTRACT = "receiver/k8sresourcereceiver/resourcecollection/"
API_FILES = {CONTRACT + "resourcecollection-crd.yaml", CONTRACT + "types.go"}
REGRESSIONS = {
    CONTRACT + "types_test.go",
    "test/resourcecollection/fixtures.py",
    "test/resourcecollection/run.py",
    "test/resourcecollection/runtime.py",
}


def git(repo, *args):
    return subprocess.check_output(["git", *args], cwd=repo, stderr=subprocess.STDOUT)


def check(repo, base, head="HEAD", worktree=False):
    if not re.fullmatch(r"[a-f0-9]{40}", base):
        raise ValueError("The baseline must be a full Git object ID")
    git(repo, "rev-parse", "--verify", base + "^{tree}")
    if head != "HEAD" and not re.fullmatch(r"[a-f0-9]{40}", head):
        raise ValueError("The candidate must be HEAD or a full Git object ID")
    revision = [] if worktree else [head]
    changed = set(git(repo, "diff", "--no-renames", "--name-only", "-z",
                      base, *revision, "--").decode().split("\0")) - {""}
    if worktree:
        changed.update(git(repo, "ls-files", "--others", "--exclude-standard", "-z")
                       .decode().split("\0"))

    def content(path):
        if worktree:
            file = repo / path
            return file.read_bytes() if file.is_file() else b""
        exists = subprocess.run(
            ["git", "cat-file", "-e", f"{head}:{path}"], cwd=repo,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return git(repo, "show", f"{head}:{path}") if exists.returncode == 0 else b""

    if not changed & API_FILES:
        return "ResourceCollection contract unchanged"
    assessments = [
        path for path in sorted(changed)
        if path.startswith(CONTRACT + "compatibility/") and path.endswith(".md")
        and content(path).strip()
    ]
    regressions = [
        path for path in sorted(changed)
        if (path in REGRESSIONS or
            (path.startswith(CONTRACT + "testdata/") and
             Path(path).suffix in {".json", ".yaml", ".yml"}))
        and content(path).strip()
    ]
    missing = []
    if not assessments:
        missing.append("a changed nonempty compatibility/*.md assessment")
    if not regressions:
        missing.append("changed admission, wire or runtime regressions")
    if missing:
        raise ValueError("ResourceCollection contract changes require " + " and ".join(missing))
    return ("ResourceCollection compatibility evidence: " +
            ", ".join(assessments + regressions))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="Git object ID to compare against")
    parser.add_argument("--worktree", action="store_true",
                        help="include uncommitted and untracked local candidate files")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    try:
        print(check(repo, args.base, worktree=args.worktree))
    except (ValueError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
