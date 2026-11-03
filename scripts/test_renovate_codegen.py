import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from renovate_codegen import OUTPUTS, save_outputs


def archive(names, kind=tarfile.REGTYPE, data=b"generated"):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as result:
        for name in names:
            member = tarfile.TarInfo(name)
            member.type = kind
            member.linkname = "/tmp/credential"
            member.size = len(data) if kind == tarfile.REGTYPE else 0
            result.addfile(member, io.BytesIO(data))
    buffer.seek(0)
    return buffer


class GeneratedFilesTests(unittest.TestCase):
    def test_only_expected_regular_files_are_written(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("renovate_codegen.Path", side_effect=lambda name: Path(directory) / name):
                for name in OUTPUTS:
                    (Path(directory) / name).parent.mkdir(parents=True, exist_ok=True)
                save_outputs(archive(OUTPUTS))
            self.assertTrue(all((Path(directory) / name).read_bytes() == b"generated" for name in OUTPUTS))

    def test_rejects_unexpected_missing_duplicate_and_linked_files(self):
        for names, kind in [
            ([*OUTPUTS, "../credential"], tarfile.REGTYPE),
            (OUTPUTS[:-1], tarfile.REGTYPE),
            ([*OUTPUTS, OUTPUTS[0]], tarfile.REGTYPE),
            (OUTPUTS, tarfile.SYMTYPE),
            (OUTPUTS, tarfile.LNKTYPE),
        ]:
            with self.subTest(names=names, kind=kind), patch("renovate_codegen.Path") as target:
                with self.assertRaises(ValueError):
                    save_outputs(archive(names, kind))
                target.assert_not_called()

    def test_rejects_oversized_files_before_writing(self):
        with patch("renovate_codegen.Path") as target:
            with self.assertRaises(ValueError):
                save_outputs(archive(OUTPUTS, data=b"x" * (8 * 1024 * 1024 + 1)))
            target.assert_not_called()

    def test_rejects_compressed_archives_before_writing(self):
        import gzip

        with patch("renovate_codegen.Path") as target:
            with self.assertRaises(tarfile.ReadError):
                save_outputs(io.BytesIO(gzip.compress(archive(OUTPUTS).getvalue())))
            target.assert_not_called()


if __name__ == "__main__":
    unittest.main()
