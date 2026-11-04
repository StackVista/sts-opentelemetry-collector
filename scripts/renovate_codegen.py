#!/usr/bin/env python3
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import uuid


OUTPUTS = (
    "extension/settingsproviderextension/generated/settingsproto/model.gen.go",
    "extension/settingsproviderextension/generated/settingsschema/model.gen.go",
    "go.work.sum",
)


def save_outputs(archive):
    with tarfile.open(fileobj=archive, mode="r:") as result:
        members = result.getmembers()
        if sorted(member.name for member in members) != sorted(OUTPUTS):
            raise ValueError("Unexpected generated files")
        if any(not member.isfile() or member.size > 8 * 1024 * 1024 for member in members):
            raise ValueError("Invalid generated file")
        contents = {member.name: result.extractfile(member).read() for member in members}
    for name, data in contents.items():
        Path(name).write_bytes(data)


def main():
    version = re.search(r"^go (\d+\.\d+\.\d+)$", Path("go.work").read_text(), re.MULTILINE)
    if version is None:
        raise ValueError("Missing workspace Go version")
    files = subprocess.check_output(["git", "ls-files", "-z"]).decode().split("\0")[:-1]
    container = f"renovate-codegen-{uuid.uuid4().hex}"
    with tempfile.TemporaryFile() as source, tempfile.TemporaryFile() as output:
        with tarfile.open(fileobj=source, mode="w") as archive:
            for name in files:
                archive.add(name, recursive=False)
        source.seek(0)
        try:
            subprocess.run(
                [
                    "docker", "run", "--rm", "--interactive", "--name", container,
                    "--cap-drop=ALL", "--security-opt=no-new-privileges",
                    "--env=GOTOOLCHAIN=local",
                    f"registry.suse.com/bci/golang:{version[1]}",
                    "sh", "-eu", "-c",
                    "mkdir /source; cd /source; tar --no-same-owner -xf -; "
                    "go -C extension/settingsproviderextension generate "
                    "./generated/settingsproto/... ./generated/settingsschema/... >&2; "
                    f"tar -cf - {' '.join(OUTPUTS)}",
                ],
                stdin=source, stdout=output, check=True, timeout=600,
            )
            output.seek(0)
            save_outputs(output)
        finally:
            subprocess.run(
                ["docker", "rm", "--force", container],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
            )


if __name__ == "__main__":
    main()
