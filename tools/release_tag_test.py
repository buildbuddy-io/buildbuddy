"""Tests for release.py tag annotations without network or Git side effects."""

import importlib.util
from pathlib import Path
import shlex
import sys
import tempfile
import types
import unittest
from unittest import mock


class ReleaseTagTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        release_path = Path(__file__).resolve().parent.parent / "release.py"
        spec = importlib.util.spec_from_file_location("release_under_test", release_path)
        self.release = importlib.util.module_from_spec(spec)
        # These tests exercise only tagging. Stub the unused network import
        # instead of pulling requests and its dependencies into this py_test.
        with mock.patch.dict(sys.modules, {"requests": types.ModuleType("requests")}):
            spec.loader.exec_module(self.release)

    def create_tag(self, branch, notes=""):
        annotations = []
        real_temporary_file = tempfile.NamedTemporaryFile

        def temporary_file(*args, **kwargs):
            return real_temporary_file(*args, dir=self.root, **kwargs)

        def run(cmd, capture_stdout=False):
            if cmd == "git rev-parse --abbrev-ref HEAD":
                self.assertTrue(capture_stdout)
                return types.SimpleNamespace(stdout=branch + "\n")
            self.assertFalse(capture_stdout)
            args = shlex.split(cmd)
            if args[:3] == ["git", "tag", "-a"]:
                self.assertEqual(["v2.10.0", "-F"], args[3:5])
                message_file = Path(args[5])
                self.assertEqual(self.root, message_file.parent)
                annotations.append(message_file.read_text())
            return types.SimpleNamespace(stdout="")

        with mock.patch.object(
            self.release.tempfile, "NamedTemporaryFile", side_effect=temporary_file
        ), mock.patch.object(self.release, "run_or_die", side_effect=run) as run_or_die:
            self.release.create_and_push_tag(
                "v2.9.0", "v2.10.0", release_notes=notes
            )
        self.assertEqual(1, len(annotations))
        commands = [call.args[0] for call in run_or_die.call_args_list]
        self.assertEqual("git rev-parse --abbrev-ref HEAD", commands[0])
        self.assertEqual(3, len(commands))
        self.assertEqual("git push origin v2.10.0", commands[-1])
        return annotations[0]

    def test_release_branch_annotation_has_exact_final_owner_line(self):
        annotation = self.create_tag("bb_release_2026_09-28", notes="Release notes")
        self.assertEqual(
            "Bump tag v2.9.0 -> v2.10.0 (release.py)\n"
            "Release notes\n\nRelease-Branch: bb_release_2026_09-28",
            annotation,
        )

    def test_branch_owner_is_appended_after_release_notes(self):
        notes = "Notes mention another cut\n\nRelease-Branch: bb_release_other"
        annotation = self.create_tag("bb_release_current", notes=notes)
        self.assertIn(notes, annotation)
        self.assertEqual(
            "Release-Branch: bb_release_current", annotation.splitlines()[-1]
        )

    def test_non_release_and_detached_checkouts_do_not_add_owner(self):
        for branch in ("master", "HEAD", "feature", "feature_bb_release_1"):
            with self.subTest(branch=branch):
                self.assertEqual(
                    "Bump tag v2.9.0 -> v2.10.0 (release.py)",
                    self.create_tag(branch),
                )

    def test_empty_release_notes_still_writes_branch_metadata(self):
        annotation = self.create_tag("bb_release_current")
        self.assertEqual(
            "Release-Branch: bb_release_current", annotation.splitlines()[-1]
        )


if __name__ == "__main__":
    unittest.main()
