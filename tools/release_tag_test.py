"""Integration tests for release.py annotations and CLI, without HTTP calls."""

import contextlib
import io
import os

import sys
import unittest
from unittest import mock

from tools.release_test_utils import ReleaseRepository


class ReleaseTagTest(ReleaseRepository, unittest.TestCase):
    def annotation(self, tag="v2.9.1"):
        return self.git(self.origin, "for-each-ref", "--format=%(contents)", f"refs/tags/{tag}")

    def test_release_branch_annotation_has_exact_final_owner_line(self):
        self.prepare_patch()
        with mock.patch.object(self.release, "run_or_die", side_effect=AssertionError("Use direct Git")):
            self.create_tag(release_notes="Release notes")
        self.assertEqual(
            "Bump tag v2.9.0 -> v2.9.1 (release.py)\n"
            "Release notes\n\nRelease-Branch: bb_release_1",
            self.annotation(),
        )
        self.assert_remote_tag_branch("v2.9.1", "bb_release_1")

    def test_branch_owner_is_appended_after_release_notes(self):
        self.prepare_patch()
        notes = "Notes mention another cut\n\nRelease-Branch: bb_release_other"
        self.create_tag(release_notes=notes)
        self.assertIn(notes, self.annotation())
        self.assertEqual("Release-Branch: bb_release_1", self.annotation().splitlines()[-1])

    def test_empty_notes_still_write_branch_metadata(self):
        self.prepare_patch()
        self.create_tag()
        self.assertEqual("Release-Branch: bb_release_1", self.annotation().splitlines()[-1])

    def test_explicit_owner_in_detached_checkout(self):
        after = self.prepare_patch()
        self.git(self.worker, "checkout", "--detach", after)
        self.create_tag(branch="bb_release_1")
        self.assert_remote_tag("v2.9.1", after)
        self.assert_remote_tag_branch("v2.9.1", "bb_release_1")

    def test_non_release_and_detached_checkouts_do_not_add_owner(self):
        after = self.prepare_patch()
        for index, branch in enumerate(("master", "HEAD", "feature", "feature_bb_release_1"), 1):
            with self.subTest(branch=branch):
                if branch == "HEAD":
                    self.git(self.worker, "checkout", "--detach", after)
                else:
                    self.git(self.worker, "checkout", "-b", branch, after)
                tag = f"v2.9.{index}"
                self.create_tag(new_version=tag)
                self.assertEqual(f"Bump tag v2.9.0 -> {tag} (release.py)", self.annotation(tag))

    def test_global_version_is_numeric_not_annotation_date(self):
        self.git(self.writer, "checkout", "-b", "master")
        head = self.commit("Version sources")
        # Simulate a later patch in an old series after the newest minor cut.
        with mock.patch.dict(os.environ, {"GIT_COMMITTER_DATE": "2026-09-26T00:00:00Z"}):
            self.publish_tag("v2.10.0", head)
        with mock.patch.dict(os.environ, {"GIT_COMMITTER_DATE": "2026-09-28T00:00:00Z"}):
            self.publish_tag("v2.9.99", head)
        for name in ("v100.0.0-rc1", "cli-v100.0.0", "v100.0", "v100.x.0"):
            self.publish_tag(name, head)
        self.git(self.writer, "push", "origin", "master")
        self.clone_worker()
        self.git(self.worker, "checkout", "master")
        with self.in_repository(self.worker):
            self.assertEqual("v2.10.0", self.release.get_latest_remote_version())
        self.run_main("--auto", "--force", "--bump_version_type=minor")
        self.assert_remote_tag("v2.11.0", head)
        self.assertEqual("version_tag=v2.11.0\n", (self.root / "outputs").read_text())
        self.requests.get.assert_not_called()

    def test_global_version_orders_major_minor_and_patch_numerically(self):
        for tag in ("v9.99.99", "v10.1.9", "v10.1.10", "v10.0.999"):
            self.publish_tag(tag, self.initial)
        self.clone_worker()
        with self.in_repository(self.worker):
            self.assertEqual("v10.1.10", self.release.get_latest_remote_version())

    def test_cli_minor_in_detached_checkout_records_explicit_cut_owner(self):
        self.clone_worker()
        self.git(self.worker, "checkout", "--detach", self.initial)
        self.run_main(
            "--auto", "--force", "--bump_version_type=minor",
            "--release_branch=bb_release_2",
        )
        self.assert_remote_tag("v2.10.0", self.initial)
        self.assert_remote_tag_branch("v2.10.0", "bb_release_2")
        self.assertEqual(
            "version_tag=v2.10.0\n", (self.root / "outputs").read_text()
        )
        self.requests.get.assert_not_called()

    def test_cli_patch_infers_normal_named_branch_and_skips_http_with_force(self):
        after = self.prepare_patch()
        self.run_main("--auto", "--force", "--bump_version_type=patch")
        self.assert_remote_tag("v2.9.1", after)
        self.assert_remote_tag_branch("v2.9.1", "bb_release_1")
        self.assertEqual("version_tag=v2.9.1\n", (self.root / "outputs").read_text())
        self.requests.get.assert_not_called()

    def test_cli_event_sha_and_retry_outputs_same_version(self):
        self.commit("First commit in push")
        after = self.commit("Last commit in event")
        self.push_branch()
        later = self.commit("Later push already moved branch")
        self.push_branch()
        self.clone_worker()
        self.git(self.worker, "checkout", "--detach", after)
        args = ("--auto", "--force", "--bump_version_type=patch",
                "--release_branch=bb_release_1", f"--previous_commit={self.initial}")
        self.run_main(*args)
        tags = self.remote_tags()
        self.run_main(*args)
        fresh = self.clone_worker(self.root / "retry")
        self.git(fresh, "checkout", "--detach", after)
        self.run_main(*args, repository=fresh)
        self.assertEqual(tags, self.remote_tags())
        self.assert_remote_tag("v2.9.1", after)
        self.assert_remote_tag_branch("v2.9.1", "bb_release_1")
        self.assertEqual("", self.git(self.origin, "tag", "--points-at", later))
        self.assertEqual("version_tag=v2.9.1\n" * 3, (self.root / "outputs").read_text())
        self.requests.get.assert_not_called()

    def test_cli_patch_never_falls_back_to_legacy_tag_for_inferred_branch(self):
        self.replace_initial_tag()
        self.prepare_patch()
        tags = self.remote_tags()
        with self.assertRaises((ValueError, SystemExit)):
            self.run_main("--auto", "--force", "--bump_version_type=patch")
        self.assertEqual(tags, self.remote_tags())
        self.assertFalse((self.root / "outputs").exists())
        self.requests.get.assert_not_called()

    def test_cli_patch_rejects_non_release_branch(self):
        self.prepare_patch()
        self.git(self.worker, "checkout", "-b", "master")
        tags = self.remote_tags()
        with self.assertRaises((ValueError, SystemExit)):
            self.run_main("--auto", "--force", "--bump_version_type=patch")
        self.assertEqual(tags, self.remote_tags())

    def test_cli_previous_commit_incompatible_flags_are_parser_errors(self):
        self.prepare_patch()
        tags = self.remote_tags()
        cases = (
            ("--auto",),
            ("--auto", "--bump_version_type=minor"),
            ("--auto", "--bump_version_type=major"),
            ("--auto", "--bump_version_type=none"),
            ("--bump_version_type=patch",),
            ("--auto", "--bump_version_type=patch", "--version=v2.9.1"),
        )
        for flags in cases:
            with self.subTest(flags=flags), contextlib.redirect_stderr(io.StringIO()):
                with mock.patch.object(self.release, "workspace_is_clean",
                                       side_effect=AssertionError("Reject before Git")):
                    with self.assertRaises(SystemExit) as error:
                        self.run_main(*flags, f"--previous_commit={self.initial}")
                self.assertEqual(2, error.exception.code)
        self.assertEqual(tags, self.remote_tags())
        self.requests.get.assert_not_called()

    def test_cli_optional_image_update_and_output_use_new_version(self):
        after = self.prepare_patch()
        with mock.patch.object(self.release, "update_docker_images") as update:
            self.run_main("--auto", "--force", "--bump_version_type=patch",
                          "--update_app_image", "--update_executor_image", "--skip_latest_tag",
                          "--arch_specific_executor_tag")
        update.assert_called_once_with(
            ["buildbuddy-app-onprem", "buildbuddy-executor-enterprise"],
            "v2.9.1", True, True, False,
        )
        self.assert_remote_tag("v2.9.1", after)
        self.assertEqual("version_tag=v2.9.1\n", (self.root / "outputs").read_text())

    def test_cli_explicit_version_does_not_create_tag(self):
        self.clone_worker()
        tags = self.remote_tags()
        with mock.patch.object(self.release, "update_docker_images") as update:
            self.run_main("--auto", "--force", "--version=v2.4.5", "--update_app_image")
        self.assertEqual(tags, self.remote_tags())
        update.assert_called_once_with(["buildbuddy-app-onprem"], "v2.4.5", False, False, False)
        self.assertEqual("version_tag=v2.4.5\n", (self.root / "outputs").read_text())
        self.requests.get.assert_not_called()

    def test_existing_remote_tag_does_not_overwrite_conflicting_local_tag(self):
        after = self.prepare_patch()
        self.publish_tag(
            "v2.9.1", after,
            message="Existing release\n\nRelease-Branch: bb_release_1",
        )
        self.git(self.worker, "tag", "v2.9.1", self.initial)
        tags = self.remote_tags()
        # The remote already has the exact result. Idempotency must not update
        # the conflicting local ref even when the tag need not be pushed.
        self.create_tag()
        self.assertEqual(tags, self.remote_tags())
        self.assertEqual(self.initial, self.git(self.worker, "rev-parse", "v2.9.1^{commit}"))

    def test_same_head_minor_tag_requires_exact_remote_owner(self):
        after = self.prepare_patch()
        cases = (
            ("v2.10.0", True, "Other cut\n\nRelease-Branch: bb_release_2"),
            ("v2.11.0", True, "Unowned cut"),
            ("v2.12.0", False, None),
        )
        for tag, annotated, message in cases:
            with self.subTest(tag=tag):
                self.publish_tag(tag, after, annotated=annotated, message=message)
                tags = self.remote_tags()
                with self.assertRaises(ValueError):
                    self.create_tag(new_version=tag, branch="bb_release_1")
                self.assertEqual(tags, self.remote_tags())
                self.assert_remote_tag(tag, after)
                # Inspect the remote annotation without creating a local tag.
                self.assertEqual("", self.git(self.worker, "tag", "-l", tag))

    def test_same_head_minor_push_race_requires_exact_remote_owner(self):
        after = self.prepare_patch()
        real_git = self.release.git
        cases = (
            ("v2.10.0", True, "Other cut\n\nRelease-Branch: bb_release_2"),
            ("v2.11.0", True, "Unowned cut"),
            ("v2.12.0", False, None),
        )
        for tag, annotated, message in cases:
            competing_tags = []

            def race(*args):
                if args[0] == "push":
                    self.publish_tag(
                        tag, after, annotated=annotated, message=message
                    )
                    competing_tags.append(self.remote_tags())
                return real_git(*args)

            with self.subTest(tag=tag), mock.patch.object(
                self.release, "git", side_effect=race
            ):
                with self.assertRaises(ValueError):
                    self.create_tag(new_version=tag, branch="bb_release_1")
            self.assertEqual([self.remote_tags()], competing_tags)
            self.assert_remote_tag(tag, after)
            # The losing local annotation must not pass as the remote owner,
            # nor may fetching the winner replace the local tag.
            self.assertEqual(
                "Release-Branch: bb_release_1",
                self.git(
                    self.worker, "for-each-ref", "--format=%(contents)",
                    f"refs/tags/{tag}",
                ).splitlines()[-1],
            )

    def test_same_owner_minor_push_race_is_idempotent(self):
        after = self.prepare_patch()
        real_git = self.release.git
        competing_tags = []

        def race(*args):
            if args[0] == "push":
                self.publish_tag(
                    "v2.10.0", after,
                    message="Same cut\n\nRelease-Branch: bb_release_1",
                )
                competing_tags.append(self.remote_tags())
            return real_git(*args)

        with mock.patch.object(self.release, "git", side_effect=race):
            self.assertIsNone(self.create_tag(new_version="v2.10.0"))
        self.assertEqual([self.remote_tags()], competing_tags)
        self.assert_remote_tag("v2.10.0", after)
        self.assert_remote_tag_branch("v2.10.0", "bb_release_1")
        # Existing-remote path also accepts only the same branch's result.
        self.create_tag(new_version="v2.10.0")
        self.assertEqual([self.remote_tags()], competing_tags)


if __name__ == "__main__":
    unittest.main()
