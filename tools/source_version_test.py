"""Unit and subprocess CLI tests; no checkout VERSION or Git state is needed."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from tools import source_version


VALID_CONTENTS = (
    (b"v0.0.0", "v0.0.0"),
    (b"v1.2.3\n", "v1.2.3"),
    (b"v10.20.300-dev", "v10.20.300-dev"),
    (b"v0.0.0-dev\n", "v0.0.0-dev"),
    (b"v1.2.3\r\n", "v1.2.3"),
    (b"v1.2.3-dev\r\n", "v1.2.3-dev"),
)
INVALID_CONTENTS = (
    b"",
    b"\n",
    b"1.2.3",
    b"V1.2.3",
    b"v1.2",
    b"v1.2.3.4",
    b"v01.2.3",
    b"v1.02.3",
    b"v1.2.03-dev",
    b"v-1.2.3",
    b"v+1.2.3",
    b"v1.2.3-rc1",
    b"v1.2.3-DEV",
    b"v1.2.3-dev.1",
    b"v1.2.3+build",
    b" v1.2.3",
    b"v1.2.3 ",
    b"v1.2.3\t",
    b"v1.2.3\r",
    b"v1.2.3\r\n\r\n",
    b"v1.2.3\r\n\n",
    b"v1.2.3\n\n",
    b"\nv1.2.3",
    b"v1.2.3\nv4.5.6",
    b"v1.2.3\x00",
    b"\xef\xbb\xbfv1.2.3",
    "v١.2.3".encode("utf-8"),
    b"v1.2.\xff",
)


class ReadVersionTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / "VERSION"

    def test_valid_versions(self):
        for content, expected in VALID_CONTENTS:
            with self.subTest(content=content):
                self.path.write_bytes(content)
                self.assertEqual(source_version.read_version(self.path), expected)
                self.assertEqual(source_version.read_version(str(self.path)), expected)

    def test_invalid_versions(self):
        for content in INVALID_CONTENTS:
            with self.subTest(content=content):
                self.path.write_bytes(content)
                with self.assertRaisesRegex(ValueError, "expected vMAJOR"):
                    source_version.read_version(self.path)

    def test_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            source_version.read_version(self.path)

    def test_unreadable_file(self):
        with mock.patch.object(Path, "read_bytes", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                source_version.read_version(self.path)

    def test_directory_is_not_a_version_file(self):
        with self.assertRaises(OSError):
            source_version.read_version(self.path.parent)


class SourceVersionCLITest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        tools = self.root / "tools"
        tools.mkdir()
        self.script = tools / "source_version.py"
        shutil.copyfile(source_version.__file__, self.script)
        self.version = self.root / "VERSION"
        self.cwd = self.root / "unrelated"
        self.cwd.mkdir()
        # A valid cwd VERSION must not hide a missing/invalid checkout VERSION.
        (self.cwd / "VERSION").write_text("v99.99.99\n")

    def run_cli(self, *args, env=None):
        return subprocess.run(
            [sys.executable, str(self.script), *map(str, args)],
            cwd=self.cwd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def assert_failure(self, result, path):
        self.assertEqual(result.returncode, 1, result)
        self.assertEqual(result.stdout, "")
        self.assertIn("source_version:", result.stderr)
        self.assertIn(str(path), result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_default_path_is_cwd_independent(self):
        for content, expected in VALID_CONTENTS:
            with self.subTest(content=content):
                self.version.write_bytes(content)
                result = self.run_cli()
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, expected + "\n")
                self.assertEqual(result.stderr, "")

    def test_imported_helper_default_is_cwd_independent(self):
        self.version.write_bytes(b"v2.3.4-dev\n")
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; sys.path.insert(0, sys.argv[1]); "
                "from source_version import read_version; print(read_version())",
                str(self.script.parent),
            ],
            cwd=self.cwd,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "v2.3.4-dev\n")
        self.assertEqual(result.stderr, "")

    def test_explicit_absolute_and_relative_paths(self):
        explicit = self.cwd / "other-version"
        explicit.write_bytes(b"v3.4.5-dev")
        for path in (explicit, explicit.name):
            with self.subTest(path=path):
                result = self.run_cli(path)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, "v3.4.5-dev\n")
                self.assertEqual(result.stderr, "")

    def test_invalid_versions(self):
        for content in INVALID_CONTENTS:
            with self.subTest(content=content):
                self.version.write_bytes(content)
                self.assert_failure(self.run_cli(), self.version)

    def test_missing_default_and_explicit_file(self):
        self.assert_failure(self.run_cli(), self.version)
        self.assert_failure(self.run_cli(self.cwd / "missing"), self.cwd / "missing")

    def test_directory_is_unreadable_as_a_file(self):
        self.assert_failure(self.run_cli(self.cwd), self.cwd)

    def test_permission_denied(self):
        self.version.write_bytes(b"v1.2.3")
        self.version.chmod(0)
        self.addCleanup(self.version.chmod, 0o600)
        if os.access(self.version, os.R_OK):
            self.skipTest("current user can read files with no read permissions")
        self.assert_failure(self.run_cli(), self.version)

    @unittest.skipUnless(os.name == "posix", "fake Git executable uses a POSIX shell")
    def test_never_falls_back_to_git(self):
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        marker = self.root / "git-called"
        git = bin_dir / "git"
        git.write_text('#!/bin/sh\nprintf called > "$GIT_MARKER"\nprintf "v88.88.88\\n"\n')
        git.chmod(0o755)
        env = dict(os.environ, PATH=str(bin_dir), GIT_MARKER=str(marker))
        for content in (None, b"not a version", b"v1.2.3\n"):
            with self.subTest(content=content):
                if content is not None:
                    self.version.write_bytes(content)
                result = self.run_cli(env=env)
                if content == b"v1.2.3\n":
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout, "v1.2.3\n")
                else:
                    self.assert_failure(result, self.version)
                self.assertFalse(marker.exists(), "reader must not invoke Git")


if __name__ == "__main__":
    unittest.main()
