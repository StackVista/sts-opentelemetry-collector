#!/usr/bin/env python3
"""Verify complete public source owners against the reviewed inventory."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1] / "third_party"
inventory = json.loads((root / "yaml-sources.json").read_text())
for name, source in inventory.items():
    directory = root / name
    expected = dict(source["original_files"])
    expected.update(source["patched_files"])
    actual = {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in directory.rglob("*") if p.is_file()}
    assert actual == expected, f"{name}: missing, added or changed source/asset"
    assert source["commit"] and source["sum"] and source["go_mod_sum"]
    assert any("license" in p.lower() for p in expected), f"{name}: license missing"
    print(f"{name}: {len(actual)} attributed files verified")
