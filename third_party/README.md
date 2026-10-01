# Collector YAML source owners

Borg owns these complete public upstream distributions under
https://github.com/StackVista/stackstate/issues/717. They preserve the exact
versions selected by both collector builders, including source APIs, tests,
licenses, generated files and UAP core assets. No private source is included.

* `uap-go`: `db9adb27a0b8fd601c670183e6e071136fe85863`, original v3
  generation; core `c941f1d2cd528be1d597471e5c502a9dc0eb3ac8`.
* `scaleway-sdk-go`: `v1.0.0-beta.36`,
  `e7da2cafb85046a408c7db17e1dae168f4e09af0`.

`yaml-sources.json` records checksum-authenticated Go ZIP provenance and every
original Git file's SHA256. The allowed source changes are matching maintained
v3/v2 imports and module metadata. Run `python3 scripts/verify-yaml-owners.py`
to reject unexplained changes, missing assets or incomplete attribution.

The OCB manifests apply replacements to each independent generated executable;
`go.work` applies the same owners to developer and component tests. Dependency
module replacements do not propagate into generated executables.

Preserve upstream generation scripts. UAP's original `build.sh` updates its
submodule remotely: do not use it for offline reproduction of this snapshot.
Reproduce `uaparser/yaml.go` from the pinned core by running its filtering and
formatting steps without the update command, and compare the bytes.

On dependency upgrades, verify the new exact upstream distribution and assets,
run original and candidate owner/caller tests, update this inventory, then build
both collectors for both architectures. Remove a replacement when a compatible
upstream release uses maintained YAML and has equivalent caller qualification.
Review these owners with each collector dependency update; do not silently
substitute a different parser generation or Scaleway release.
