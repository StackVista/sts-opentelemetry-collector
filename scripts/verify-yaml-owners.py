#!/usr/bin/env python3
"""Verify complete public source owners against the reviewed inventory."""
import hashlib
import json
import tarfile
from pathlib import Path

root = Path(__file__).resolve().parents[1] / "third_party"
inventory = json.loads((root / "yaml-sources.json").read_text())
identities = {
    "uap-go": ("db9adb27a0b8fd601c670183e6e071136fe85863", "c941f1d2cd528be1d597471e5c502a9dc0eb3ac8"),
    "scaleway-sdk-go": ("e7da2cafb85046a408c7db17e1dae168f4e09af0", None),
}
assert set(inventory) == set(identities), "unexpected owner distribution"
for name, source in inventory.items():
    assert (source["commit"], source["core_commit"]) == identities[name], "wrong source generation"
    assert set(source["patched_files"]) <= set(source["original_files"]), "unattributed patch"
    assert all(p in {"go.mod", "go.sum"} or p.endswith((".go", ".md"))
               for p in source["patched_files"]), "asset or workflow patch is not permitted"
    directory = root / name
    expected = dict(source["original_files"])
    expected.update(source["patched_files"])
    actual = {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in directory.rglob("*") if p.is_file()}
    for archive in directory.rglob("UPSTREAM-GITHUB.tar"):
        actual.pop(str(archive.relative_to(directory)))
        prefix = archive.parent.relative_to(directory)
        with tarfile.open(archive) as archived:
            for member in archived.getmembers():
                if member.isfile():
                    path = str(prefix / member.name)
                    assert path not in actual, f"duplicate archived source: {path}"
                    actual[path] = hashlib.sha256(archived.extractfile(member).read()).hexdigest()
    assert actual == expected, f"{name}: missing, added or changed source/asset"
    assert source["commit"] and source["sum"] and source["go_mod_sum"]
    assert any("license" in p.lower() for p in expected), f"{name}: license missing"
    print(f"{name}: {len(actual)} attributed files verified")
