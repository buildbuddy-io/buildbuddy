"""Safety tests for release.py patch tagging against a temporary Git origin."""

import subprocess
import unittest
from unittest import mock

from tools.release_test_utils import ReleaseRepository


class TagReleaseBranchTest(ReleaseRepository, unittest.TestCase):
    def test_uses_branch_version_not_newer_global_version(self):
        self.git(self.writer, "checkout", "-b", "master")
        master = self.commit("Newer release on master")
        self.publish_tag("v99.0.0", master)
        self.git(self.writer, "push", "origin", "master")
        self.git(self.writer, "checkout", "bb_release_1")
        after = self.prepare_patch()

        self.assertEqual("v2.9.1", self.patch_release(self.initial))
        self.assert_remote_tag("v2.9.1", after)
        self.assert_remote_tag("v99.0.0", master)
        self.assert_remote_tag_branch("v2.9.1", "bb_release_1")

    def test_ignores_non_version_tags_at_prior_head(self):
        for name in ("v99.0.0-rc1", "cli-v99.0.0", "v99.0", "v99.x.0"):
            self.publish_tag(name, self.initial,
                             message="Not semver\n\nRelease-Branch: bb_release_1")
        after = self.prepare_patch()
        self.assertEqual("v2.9.1", self.patch_release(self.initial))
        self.assert_remote_tag("v2.9.1", after)

    def test_branch_metadata_selects_version_at_shared_initial_sha(self):
        branch = "bb_release_1"
        self.replace_initial_tag()
        self.publish_tag("v2.10.0", self.initial,
                         message=f"Initial release\n\nRelease-Branch: {branch}")
        for name, annotation in (
            ("v2.11.0", "Other branch\n\nRelease-Branch: bb_release_2"),
            ("v2.12.0", "Prefix match is not exact\n\nRelease-Branch: bb_release_10"),
            ("v2.13.0", f"Not a footer\n\nRelease-Branch: {branch}\nAdditional text"),
            ("v2.14.0", f"Suffix match is not exact\n\nRelease-Branch: {branch}-other"),
        ):
            self.publish_tag(name, self.initial, message=annotation)
        self.publish_tag("v2.15.0", self.initial, annotated=False)
        after = self.prepare_patch()
        self.assertEqual("v2.10.1", self.patch_release(self.initial))
        self.assert_remote_tag("v2.10.1", after)
        self.assert_remote_tag_branch("v2.10.1", branch)

    def test_subsequent_patch_preserves_branch_metadata(self):
        branch = "bb_release_1"
        self.replace_initial_tag()
        self.publish_tag("v2.10.0", self.initial,
                         message=f"Initial release\n\nRelease-Branch: {branch}")
        self.publish_tag("v2.11.0", self.initial,
                         message="Other cut\n\nRelease-Branch: bb_release_2")
        first = self.prepare_patch()
        self.assertEqual("v2.10.1", self.patch_release(self.initial))
        self.assert_remote_tag_branch("v2.10.1", branch)
        self.publish_tag("v2.99.0", first,
                         message="Other branch\n\nRelease-Branch: bb_release_2")
        second = self.commit("Second push")
        self.push_branch()
        self.git(self.worker, "fetch", "origin", "bb_release_1")
        self.git(self.worker, "checkout", "--detach", second)

        self.assertEqual("v2.10.2", self.patch_release(first))
        self.assert_remote_tag("v2.10.2", second)
        self.assert_remote_tag_branch("v2.10.2", branch)

    def test_unowned_legacy_tags_fail_with_and_without_previous_commit(self):
        self.replace_initial_tag()
        self.prepare_patch()
        tags = self.remote_tags()
        for previous in (self.initial, None):
            with self.subTest(previous_commit=previous):
                with mock.patch.object(self.release.time, "sleep") as sleep:
                    with self.assertRaises(ValueError):
                        self.patch_release(previous)
                sleep.assert_not_called()
        self.assertEqual(tags, self.remote_tags())

    def test_waits_for_matching_owner_despite_other_tags_at_shared_sha(self):
        self.replace_initial_tag()
        self.publish_tag("v2.11.0", self.initial,
                         message="Another cut\n\nRelease-Branch: bb_release_2")
        after = self.prepare_patch()

        def publish_matching_tag(interval):
            self.assertEqual(7, interval)
            self.publish_tag("v2.10.0", self.initial,
                             message="Pending cut\n\nRelease-Branch: bb_release_1")

        with mock.patch.object(self.release.time, "sleep", side_effect=publish_matching_tag) as sleep:
            self.assertEqual("v2.10.1", self.patch_release(self.initial, attempts=3))
        sleep.assert_called_once_with(7)
        self.assert_remote_tag("v2.10.1", after)
        self.assert_remote_tag_branch("v2.10.1", "bb_release_1")

    def test_lightweight_tag_cannot_use_commit_message_as_branch_metadata(self):
        before = self.commit("Commit message, not tag metadata\n\nRelease-Branch: bb_release_1")
        self.publish_tag("v2.10.0", before, annotated=False)
        self.prepare_patch()
        tags = self.remote_tags()
        with self.assertRaises(ValueError):
            self.patch_release(before)
        self.assertEqual(tags, self.remote_tags())

    def test_multiple_unowned_tags_fail(self):
        self.replace_initial_tag()
        self.publish_tag("v2.10.0", self.initial)
        self.prepare_patch()
        tags = self.remote_tags()
        with self.assertRaises(ValueError):
            self.patch_release(self.initial)
        self.assertEqual(tags, self.remote_tags())

    def test_lightweight_base_tags_fail(self):
        self.replace_initial_tag(annotated=False)
        self.publish_tag("v2.10.0", self.initial, annotated=False)
        self.prepare_patch()
        tags = self.remote_tags()
        for previous in (self.initial, None):
            with self.subTest(previous_commit=previous):
                with self.assertRaises(ValueError):
                    self.patch_release(previous)
        self.assertEqual(tags, self.remote_tags())

    def test_sequential_pushes_increment_patch(self):
        first = self.prepare_patch()
        self.assertEqual("v2.9.1", self.patch_release(self.initial))
        second = self.commit("Second push")
        self.push_branch()
        self.git(self.worker, "fetch", "origin", "bb_release_1")
        self.git(self.worker, "checkout", "--detach", second)
        self.assertEqual("v2.9.2", self.patch_release(first))
        self.assert_remote_tag("v2.9.1", first)
        self.assert_remote_tag("v2.9.2", second)

    def test_retry_is_idempotent_in_same_and_fresh_clone(self):
        after = self.prepare_patch()
        self.assertEqual("v2.9.1", self.patch_release(self.initial))
        tags = self.remote_tags()
        self.assertEqual("v2.9.1", self.patch_release(self.initial))
        fresh = self.clone_worker(self.root / "retry")
        self.assertEqual("v2.9.1", self.patch_release(self.initial, repository=fresh))
        self.assertEqual(tags, self.remote_tags())
        self.assert_remote_tag("v2.9.1", after)

    def test_multi_commit_push_tags_checked_out_event_not_moving_branch(self):
        intermediate = self.commit("First commit in push")
        after = self.commit("Last commit in event")
        self.push_branch()
        later = self.commit("Later push already moved branch")
        self.push_branch()
        self.clone_worker()
        self.assertEqual(later, self.git(self.worker, "rev-parse", "HEAD"))
        self.git(self.worker, "checkout", "--detach", after)

        self.assertEqual("v2.9.1", self.patch_release(self.initial))
        self.assert_remote_tag("v2.9.1", after)
        for commit in (intermediate, later):
            self.assertEqual("", self.git(self.origin, "tag", "--points-at", commit))
        self.assertEqual(later, self.git(self.origin, "rev-parse", "refs/heads/bb_release_1"))

    def test_waits_for_prior_head_tag_published_during_sleep(self):
        before = self.commit("Prior push not yet tagged")
        after = self.prepare_patch()

        def publish_previous_tag(interval):
            self.assertEqual(7, interval)
            self.publish_tag("v2.9.1", before,
                             message="Prior push\n\nRelease-Branch: bb_release_1")

        with mock.patch.object(self.release.time, "sleep", side_effect=publish_previous_tag) as sleep:
            self.assertEqual("v2.9.2", self.patch_release(before, attempts=3))
        sleep.assert_called_once_with(7)
        self.assert_remote_tag("v2.9.1", before)
        self.assert_remote_tag("v2.9.2", after)

    def test_missing_prior_head_tag_fails_instead_of_using_ancestor_tag(self):
        before = self.commit("Untagged prior head")
        self.prepare_patch()
        tags = self.remote_tags()
        with mock.patch.object(self.release.time, "sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "No release version tag"):
                self.patch_release(before, attempts=3)
        self.assertEqual([mock.call(7), mock.call(7)], sleep.call_args_list)
        self.assertEqual(tags, self.remote_tags())

    def test_missing_initial_tag_fails(self):
        self.git(self.writer, "tag", "-d", "v2.9.0")
        self.git(self.writer, "push", "origin", ":refs/tags/v2.9.0")
        self.prepare_patch()
        with mock.patch.object(self.release.time, "sleep") as sleep:
            with self.assertRaises(ValueError):
                self.patch_release(self.initial)
        sleep.assert_not_called()
        self.assertEqual("", self.git(self.origin, "tag", "-l"))

    def test_conflicting_remote_tag_is_never_overwritten(self):
        self.prepare_patch()
        other = self.commit("Another event")
        self.publish_tag("v2.9.1", other)
        tags = self.remote_tags()
        with self.assertRaises(ValueError):
            self.patch_release(self.initial)
        self.assertEqual(tags, self.remote_tags())
        self.assert_remote_tag("v2.9.1", other)

    def test_conflicting_local_tag_is_never_overwritten(self):
        after = self.prepare_patch()
        other = self.commit("Local-only commit", repository=self.worker)
        self.git(self.worker, "tag", "v2.9.1", other)
        self.git(self.worker, "checkout", "--detach", after)
        tags = self.remote_tags()
        with self.assertRaises((ValueError, subprocess.CalledProcessError)):
            self.patch_release(self.initial)
        self.assertEqual(tags, self.remote_tags())
        self.assertEqual(other, self.git(self.worker, "rev-parse", "refs/tags/v2.9.1"))

    def test_same_event_racing_push_is_idempotent(self):
        after = self.prepare_patch()
        real_git = self.release.git
        competing_tags = []

        def race(*args):
            if args[0] == "push":
                self.publish_tag("v2.9.1", after,
                                 message="Competing run\n\nRelease-Branch: bb_release_1")
                competing_tags.append(self.remote_tags())
            return real_git(*args)

        with mock.patch.object(self.release, "git", side_effect=race):
            self.assertEqual("v2.9.1", self.patch_release(self.initial))
        self.assertEqual([self.remote_tags()], competing_tags)
        self.assert_remote_tag("v2.9.1", after)

    def test_conflicting_racing_push_is_never_overwritten(self):
        self.prepare_patch()
        other = self.commit("Another event")
        real_git = self.release.git
        competing_tags = []

        def race(*args):
            if args[0] == "push":
                self.publish_tag("v2.9.1", other)
                competing_tags.append(self.remote_tags())
            return real_git(*args)

        with mock.patch.object(self.release, "git", side_effect=race):
            with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                self.patch_release(self.initial)
        self.assertEqual([self.remote_tags()], competing_tags)
        self.assert_remote_tag("v2.9.1", other)

    def test_existing_lightweight_tag_at_event_sha_cannot_claim_branch_ownership(self):
        after = self.prepare_patch()
        self.publish_tag("v2.9.1", after, annotated=False)
        tags = self.remote_tags()
        with self.assertRaises(ValueError):
            self.patch_release(self.initial)
        self.assertEqual(tags, self.remote_tags())

    def test_invalid_previous_shas_fail_without_changing_tags(self):
        self.prepare_patch()
        tags = self.remote_tags()
        for invalid in ("0" * 39, "0" * 41, "g" * 40, "A" * 40, "HEAD"):
            with self.subTest(previous_commit=invalid):
                with self.assertRaises(ValueError):
                    self.patch_release(invalid)
        self.assertEqual(tags, self.remote_tags())

    def test_same_before_and_head_fails(self):
        self.clone_worker()
        tags = self.remote_tags()
        with self.assertRaises(ValueError):
            self.patch_release(self.initial)
        self.assertEqual(tags, self.remote_tags())

    def test_non_fast_forward_fails(self):
        before = self.commit("Old branch head")
        self.push_branch()
        self.git(self.writer, "checkout", "-b", "replacement", self.initial)
        self.commit("Divergent branch head")
        self.git(self.writer, "push", "--force", "origin", "replacement:refs/heads/bb_release_1")
        self.clone_worker()
        self.git(self.worker, "fetch", str(self.writer), before)
        tags = self.remote_tags()
        with self.assertRaises((ValueError, subprocess.CalledProcessError)):
            self.patch_release(before)
        self.assertEqual(tags, self.remote_tags())

    def test_unknown_commit_fails(self):
        self.prepare_patch()
        tags = self.remote_tags()
        with self.assertRaises((ValueError, subprocess.CalledProcessError)):
            self.patch_release("0" * 40)
        self.assertEqual(tags, self.remote_tags())

    def test_shallow_clone_fails(self):
        self.commit("Release fix")
        self.push_branch()
        self.clone_worker(shallow=True)
        self.assertEqual("true", self.git(self.worker, "rev-parse", "--is-shallow-repository"))
        tags = self.remote_tags()
        for previous in (self.initial, None):
            with self.subTest(previous_commit=previous):
                with self.assertRaisesRegex(ValueError, "Full history is required"):
                    self.patch_release(previous)
        self.assertEqual(tags, self.remote_tags())

    def test_invalid_branch_fails_with_and_without_previous_commit(self):
        self.prepare_patch()
        tags = self.remote_tags()
        for branch in ("", "master", "HEAD", "feature_bb_release_1", "bb_release_", "bb_release_a/b"):
            for previous in (self.initial, None):
                with self.subTest(branch=branch, previous_commit=previous):
                    with self.assertRaises(ValueError):
                        self.patch_release(previous, branch=branch)
        self.assertEqual(tags, self.remote_tags())

    def test_no_previous_commit_uses_highest_reachable_owned_version_without_wait(self):
        self.publish_tag("v2.9.10", self.initial,
                         message="Current cut\n\nRelease-Branch: bb_release_1")
        self.git(self.writer, "checkout", "-b", "other", self.initial)
        unrelated = self.commit("Unreachable owned tag")
        self.publish_tag("v2.99.0", unrelated,
                         message="Unreachable\n\nRelease-Branch: bb_release_1")
        self.git(self.writer, "checkout", "bb_release_1")
        after = self.prepare_patch()
        with mock.patch.object(self.release.time, "sleep") as sleep:
            self.assertEqual("v2.9.11", self.patch_release())
        sleep.assert_not_called()
        self.assert_remote_tag("v2.9.11", after)

    def test_no_previous_commit_does_not_wait_for_missing_owner(self):
        self.replace_initial_tag()
        self.prepare_patch()
        with mock.patch.object(self.release.time, "sleep") as sleep:
            with self.assertRaises(ValueError):
                self.patch_release(attempts=3)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
