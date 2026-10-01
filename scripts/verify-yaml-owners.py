#!/usr/bin/env python3
"""Verify worktree and committed public owners, including inert archive assets."""
import argparse
import hashlib
import io
import json
import subprocess
import tarfile
from pathlib import Path, PurePosixPath

IDENTITIES = {
    "uap-go": ("db9adb27a0b8fd601c670183e6e071136fe85863", "c941f1d2cd528be1d597471e5c502a9dc0eb3ac8"),
    "scaleway-sdk-go": ("e7da2cafb85046a408c7db17e1dae168f4e09af0", None),
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def committed_files(root, revision):
    tree = git(root, "rev-parse", "--verify", revision + "^{tree}").decode().strip()
    files = {}
    for entry in git(root, "ls-tree", "-rz", tree, "--", "third_party").split(b"\0"):
        if not entry:
            continue
        metadata, path = entry.decode().split("\t")
        mode, kind, oid = metadata.split()
        require(kind == "blob" and mode in {"100644", "100755"}, f"unsupported Git entry: {path}")
        files[path] = (git(root, "cat-file", "blob", oid), mode)
    return tree, files


def expanded(files):
    result = {}
    for path, (data, mode) in files.items():
        if PurePosixPath(path).name != "UPSTREAM-GITHUB.tar":
            require(path not in result, f"duplicate source: {path}")
            result[path] = (data, mode)
            continue
        require(mode == "100644", f"archive must be non-executable: {path}")
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            for member in archive.getmembers():
                relative = PurePosixPath(member.name)
                require(not relative.is_absolute() and ".." not in relative.parts,
                        f"unsafe archived path: {member.name}")
                if member.isdir():
                    continue
                require(member.isfile() and relative.parts[0] == ".github",
                        f"unsupported archived asset: {member.name}")
                target = str(PurePosixPath(path).parent / relative)
                require(target not in result and target not in files, f"duplicate archived source: {target}")
                # Git preserves executable status, not tar's owner/read/write bits.
                result[target] = (archive.extractfile(member).read(),
                                  "100755" if member.mode & 0o111 else "100644")
    return result


def verify_owner(name, source, files):
    require((source["commit"], source["core_commit"]) == IDENTITIES[name], "wrong source generation")
    require(set(source["patched_files"]) <= set(source["original_files"]), "unattributed patch")
    require(all(p in {"go.mod", "go.sum"} or p.endswith((".go", ".md"))
                for p in source["patched_files"]), "asset or workflow patch is not permitted")
    expected = source["original_files"] | source["patched_files"]
    actual = expanded(files)
    require(set(actual) == set(expected), f"{name}: missing or added source/asset")
    require(set(source["original_modes"]) == set(expected), f"{name}: incomplete original modes")
    for path, (data, mode) in actual.items():
        require(hashlib.sha256(data).hexdigest() == expected[path], f"{name}/{path}: changed source/asset")
        require(mode == source["original_modes"][path], f"{name}/{path}: changed Git mode")
    parser, maintained, legacy = ("v3", "v3.0.5", "v3.0.1") if name == "uap-go" else ("v2", "v2.4.4", "v2.4.0")
    for path in source["patched_files"]:
        data = actual[path][0]
        if path == "go.sum":
            data = b"".join(line for line in data.splitlines(keepends=True)
                            if not line.startswith(f"go.yaml.in/yaml/{parser} ".encode()))
        else:
            data = data.replace(f"go.yaml.in/yaml/{parser}".encode(), f"gopkg.in/yaml.{parser}".encode())
            if path == "go.mod":
                data = data.replace(f"gopkg.in/yaml.{parser} {maintained}".encode(),
                                    f"gopkg.in/yaml.{parser} {legacy}".encode())
        require(hashlib.sha256(data).hexdigest() == source["original_files"][path],
                f"{name}/{path}: non-parser source patch")
    require(source["commit"] and source["sum"] and source["go_mod_sum"], "missing provenance")
    require(any("license" in p.lower() for p in expected), f"{name}: license missing")
    return len(actual)


def verify(root, revision="HEAD"):
    tree, committed = committed_files(root, revision)
    inventory_path = "third_party/yaml-sources.json"
    inventory = json.loads(committed[inventory_path][0])
    require(set(inventory) == set(IDENTITIES), "unexpected owner distribution")
    require((root / inventory_path).read_bytes() == committed[inventory_path][0], "uncommitted inventory")
    for name, source in inventory.items():
        prefix = f"third_party/{name}/"
        files = {p[len(prefix):]: entry for p, entry in committed.items() if p.startswith(prefix)}
        directory = root / "third_party" / name
        worktree = {}
        for path in directory.rglob("*"):
            require(not path.is_symlink(), f"worktree symlink: {path}")
            if path.is_file():
                worktree[str(path.relative_to(directory))] = (
                    path.read_bytes(), "100755" if path.stat().st_mode & 0o111 else "100644")
        require(worktree == files, f"{name}: worktree differs from committed source bytes/modes/file set")
        count = verify_owner(name, source, files)
        print(f"{name}: {count} committed attributed files verified (tree {tree})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", default="HEAD", help="Git revision or tree being qualified (default HEAD)")
    args = parser.parse_args()
    verify(Path(__file__).resolve().parents[1], args.revision)
