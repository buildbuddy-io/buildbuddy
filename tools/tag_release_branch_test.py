"""Integration tests for release-branch tagging against a temporary Git origin."""

import contextlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from tools import tag_release_branch


class TagReleaseBranchTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=os.environ.get("TEST_TMPDIR"))
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        home = self.root / "home"
        templates = self.root / "templates"
        home.mkdir()
        templates.mkdir()
        # Drop inherited repository/config overrides as well as isolating HOME:
        # all Git commands, including the implementation's, use this environment.
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("GIT_")
        }
        environment.update(
            HOME=str(home),
            XDG_CONFIG_HOME=str(home / ".config"),
            XDG_CONFIG_DIRS=str(home / ".config"),
            GIT_CONFIG_NOSYSTEM="1",
            GIT_CONFIG_SYSTEM=os.devnull,
            GIT_CONFIG_GLOBAL=os.devnull,
            GIT_TEMPLATE_DIR=str(templates),
            GIT_AUTHOR_NAME="Release test",
            GIT_AUTHOR_EMAIL="release-test@example.com",
            GIT_COMMITTER_NAME="Release test",
            GIT_COMMITTER_EMAIL="release-test@example.com",
            GIT_TERMINAL_PROMPT="0",
        )
        patch = mock.patch.dict(os.environ, environment, clear=True)
        patch.start()
        self.addCleanup(patch.stop)

        self.origin = self.root / "origin.git"
        self.writer = self.root / "writer"
        self.worker = self.root / "worker"
        self.git(self.root, "init", "--bare", str(self.origin))
        self.git(self.origin, "symbolic-ref", "HEAD", "refs/heads/bb_release_1")
        self.git(self.root, "clone", str(self.origin), str(self.writer))
        self.initial = self.commit("Initial release")
        self.publish_tag("v2.9.0", self.initial)
        self.push_branch()

    def git(self, repository, *args):
        return subprocess.run(
            ["git", "-C", str(repository), *args],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.strip()

    def commit(self, message, repository=None):
        repository = repository or self.writer
        self.git(repository, "commit", "--allow-empty", "-m", message)
        return self.git(repository, "rev-parse", "HEAD")

    def push_branch(self):
        self.git(self.writer, "push", "origin", "bb_release_1")

    def publish_tag(self, name, commit, annotated=True, message=None):
        if annotated:
            self.git(self.writer, "tag", "-a", name, commit, "-m", message or name)
        else:
            self.git(self.writer, "tag", name, commit)
        self.git(self.writer, "push", "origin", f"refs/tags/{name}")

    def clone_worker(self, path=None, shallow=False):
        path = path or self.worker
        args = ["clone"]
        if shallow:
            # A file URL is required: Git ignores --depth on local-path clones.
            args.extend(["--depth", "1"])
        args.extend([self.origin.as_uri(), str(path)])
        self.git(self.root, *args)
        return path

    @contextlib.contextmanager
    def in_repository(self, repository):
        previous = Path.cwd()
        os.chdir(repository)
        try:
            yield
        finally:
            os.chdir(previous)

    def tag_push(self, before, after, repository=None, **kwargs):
        with self.in_repository(repository or self.worker):
            return tag_release_branch.tag_push(before, after, **kwargs)

    def remote_tags(self):
        # Include the annotated tag object IDs, not just their target commits,
        # so assertions detect an overwrite even if the target stays the same.
        return self.git(self.origin, "show-ref", "--tags")

    def assert_remote_tag(self, name, commit):
        self.assertEqual(
            commit, self.git(self.origin, "rev-parse", f"refs/tags/{name}^{{commit}}")
        )

    def assert_remote_tag_branch(self, name, branch):
        self.assertEqual(
            "tag", self.git(self.origin, "cat-file", "-t", f"refs/tags/{name}")
        )
        annotation = self.git(self.origin, "cat-file", "tag", f"refs/tags/{name}")
        self.assertEqual(f"Release-Branch: {branch}", annotation.splitlines()[-1])

    def test_uses_branch_version_not_newer_global_version(self):
        self.git(self.writer, "checkout", "-b", "master")
        master = self.commit("Newer release on master")
        self.publish_tag("v99.0.0", master)
        self.git(self.writer, "push", "origin", "master")
        self.git(self.writer, "checkout", "bb_release_1")
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()

        self.assertEqual("v2.9.1", self.tag_push(self.initial, after))
        self.assert_remote_tag("v2.9.1", after)
        self.assert_remote_tag("v99.0.0", master)
        self.assertEqual(
            "tag", self.git(self.origin, "cat-file", "-t", "refs/tags/v2.9.1")
        )

    def test_ignores_non_version_tags_at_prior_head(self):
        for name in (
            "v99.0.0-rc1",
            "cli-v99.0.0",
            "v99.0",
            "v99.x.0",
        ):
            self.publish_tag(name, self.initial)
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()

        self.assertEqual("v2.9.1", self.tag_push(self.initial, after))
        self.assert_remote_tag("v2.9.1", after)

    def test_branch_metadata_selects_version_at_shared_initial_sha(self):
        branch = "bb_release_1"
        self.publish_tag(
            "v2.10.0",
            self.initial,
            message=f"Initial release\n\nRelease-Branch: {branch}",
        )
        for name, annotation in (
            ("v2.11.0", "Other branch\n\nRelease-Branch: bb_release_2"),
            ("v2.12.0", "Prefix match is not exact\n\nRelease-Branch: bb_release_10"),
            (
                "v2.13.0",
                f"Not a footer\n\nRelease-Branch: {branch}\nAdditional text",
            ),
            ("v2.14.0", f"Suffix match is not exact\n\nRelease-Branch: {branch}-other"),
        ):
            self.publish_tag(name, self.initial, message=annotation)
        self.publish_tag("v2.15.0", self.initial, annotated=False)
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()

        self.assertEqual(
            "v2.10.1", self.tag_push(self.initial, after, branch=branch, attempts=1)
        )
        self.assert_remote_tag("v2.10.1", after)
        self.assert_remote_tag_branch("v2.10.1", branch)

    def test_subsequent_patch_preserves_branch_metadata(self):
        branch = "bb_release_1"
        self.publish_tag(
            "v2.10.0",
            self.initial,
            message=f"Initial release\n\nRelease-Branch: {branch}",
        )
        self.publish_tag(
            "v2.11.0",
            self.initial,
            message="Other cut at the same SHA\n\nRelease-Branch: bb_release_2",
        )
        first = self.commit("First push")
        self.push_branch()
        self.clone_worker()
        self.assertEqual(
            "v2.10.1", self.tag_push(self.initial, first, branch=branch, attempts=1)
        )
        self.assert_remote_tag_branch("v2.10.1", branch)
        self.publish_tag(
            "v2.99.0",
            first,
            message="Other branch shares the next SHA\n\nRelease-Branch: bb_release_2",
        )
        second = self.commit("Second push")
        self.push_branch()
        self.git(self.worker, "fetch", "origin", "bb_release_1")

        self.assertEqual(
            "v2.10.2", self.tag_push(first, second, branch=branch, attempts=1)
        )
        self.assert_remote_tag("v2.10.2", second)
        self.assert_remote_tag_branch("v2.10.2", branch)

    def test_explicit_branch_requires_metadata_even_with_single_legacy_tag(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        tags = self.remote_tags()

        with self.assertRaisesRegex(ValueError, "No release version tag"):
            self.tag_push(self.initial, after, branch="bb_release_1", attempts=1)
        self.assertEqual(tags, self.remote_tags())

    def test_single_legacy_tag_is_accepted_without_branch(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()

        self.assertEqual(
            "v2.9.1", self.tag_push(self.initial, after, branch=None, attempts=1)
        )
        self.assert_remote_tag("v2.9.1", after)

    def test_waits_for_matching_owner_despite_other_tags_at_shared_sha(self):
        branch = "bb_release_1"
        # The initial SHA already has one unowned legacy tag. Neither it nor
        # another branch's owned tag is enough to choose this branch's version.
        self.publish_tag(
            "v2.11.0",
            self.initial,
            message="Another initial cut\n\nRelease-Branch: bb_release_2",
        )
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()

        def publish_matching_tag(interval):
            self.assertEqual(7, interval)
            self.publish_tag(
                "v2.10.0",
                self.initial,
                message=f"Pending initial cut\n\nRelease-Branch: {branch}",
            )

        with mock.patch.object(
            tag_release_branch.time, "sleep", side_effect=publish_matching_tag
        ) as sleep:
            self.assertEqual(
                "v2.10.1",
                self.tag_push(
                    self.initial, after, branch=branch, attempts=3, interval=7
                ),
            )
        sleep.assert_called_once_with(7)
        self.assert_remote_tag("v2.10.1", after)
        self.assert_remote_tag_branch("v2.10.1", branch)

    def test_lightweight_tag_cannot_use_commit_message_as_branch_metadata(self):
        branch = "bb_release_1"
        before = self.commit(
            f"Commit message, not tag metadata\n\nRelease-Branch: {branch}"
        )
        self.publish_tag("v2.10.0", before, annotated=False)
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        tags = self.remote_tags()

        with self.assertRaisesRegex(ValueError, "No release version tag"):
            self.tag_push(before, after, branch=branch, attempts=1)
        self.assertEqual(tags, self.remote_tags())

    def test_ambiguous_legacy_annotated_versions_fail(self):
        # Old annotated tags without branch metadata cannot distinguish cuts.
        self.publish_tag("v2.10.0", self.initial)
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        tags = self.remote_tags()
        for branch in (None, "bb_release_1"):
            with self.subTest(branch=branch):
                with self.assertRaises(ValueError):
                    self.tag_push(self.initial, after, branch=branch, attempts=1)
        self.assertEqual(tags, self.remote_tags())

    def test_ambiguous_lightweight_versions_fail(self):
        self.git(self.writer, "tag", "-d", "v2.9.0")
        self.git(self.writer, "push", "origin", ":refs/tags/v2.9.0")
        self.publish_tag("v2.9.0", self.initial, annotated=False)
        self.publish_tag("v2.10.0", self.initial, annotated=False)
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        tags = self.remote_tags()
        for branch in (None, "bb_release_1"):
            with self.subTest(branch=branch):
                with self.assertRaises(ValueError):
                    self.tag_push(self.initial, after, branch=branch, attempts=1)
        self.assertEqual(tags, self.remote_tags())

    def test_sequential_pushes_increment_patch(self):
        first = self.commit("First push")
        self.push_branch()
        self.clone_worker()
        self.assertEqual("v2.9.1", self.tag_push(self.initial, first))

        second = self.commit("Second push")
        self.push_branch()
        self.git(self.worker, "fetch", "origin", "bb_release_1")
        self.assertEqual("v2.9.2", self.tag_push(first, second))
        self.assert_remote_tag("v2.9.1", first)
        self.assert_remote_tag("v2.9.2", second)

    def test_retry_is_idempotent_in_same_and_fresh_clone(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        self.assertEqual("v2.9.1", self.tag_push(self.initial, after))
        tags = self.remote_tags()

        self.assertEqual("v2.9.1", self.tag_push(self.initial, after))
        fresh = self.clone_worker(self.root / "retry")
        self.assertEqual(
            "v2.9.1", self.tag_push(self.initial, after, repository=fresh)
        )
        self.assertEqual(tags, self.remote_tags())

    def test_multi_commit_push_tags_only_event_sha_despite_moving_branch(self):
        intermediate = self.commit("First commit in push")
        after = self.commit("Last commit in event")
        self.push_branch()
        later = self.commit("Subsequent push has already moved the branch")
        self.push_branch()
        self.clone_worker()
        self.assertEqual(later, self.git(self.worker, "rev-parse", "HEAD"))

        self.assertEqual("v2.9.1", self.tag_push(self.initial, after))
        self.assert_remote_tag("v2.9.1", after)
        for commit in (intermediate, later):
            self.assertEqual(
                "", self.git(self.origin, "tag", "--points-at", commit)
            )
        self.assertEqual(
            later, self.git(self.origin, "rev-parse", "refs/heads/bb_release_1")
        )

    def test_waits_for_prior_head_tag_published_during_sleep(self):
        before = self.commit("Prior push not yet tagged")
        after = self.commit("Current push")
        self.push_branch()
        self.clone_worker()

        def publish_previous_tag(interval):
            self.assertEqual(7, interval)
            self.publish_tag("v2.9.1", before)

        with mock.patch.object(
            tag_release_branch.time, "sleep", side_effect=publish_previous_tag
        ) as sleep:
            self.assertEqual(
                "v2.9.2", self.tag_push(before, after, attempts=3, interval=7)
            )
        sleep.assert_called_once_with(7)
        self.assert_remote_tag("v2.9.1", before)
        self.assert_remote_tag("v2.9.2", after)

    def test_missing_prior_head_tag_fails_instead_of_using_ancestor_tag(self):
        before = self.commit("Untagged prior head")
        after = self.commit("Current push")
        self.push_branch()
        self.clone_worker()
        tags = self.remote_tags()

        with mock.patch.object(tag_release_branch.time, "sleep") as sleep:
            with self.assertRaisesRegex(
                ValueError, f"No release version tag at {before}"
            ):
                self.tag_push(before, after, attempts=3, interval=7)
        self.assertEqual([mock.call(7), mock.call(7)], sleep.call_args_list)
        self.assertEqual(tags, self.remote_tags())

    def test_missing_initial_tag_fails(self):
        self.git(self.writer, "tag", "-d", "v2.9.0")
        self.git(self.writer, "push", "origin", ":refs/tags/v2.9.0")
        after = self.commit("First release push")
        self.push_branch()
        self.clone_worker()
        with mock.patch.object(tag_release_branch.time, "sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "repair the initial cut"):
                self.tag_push(self.initial, after, attempts=1)
        sleep.assert_not_called()
        self.assertEqual("", self.git(self.origin, "tag", "-l"))

    def test_conflicting_remote_tag_is_never_overwritten(self):
        after = self.commit("Release fix")
        self.push_branch()
        other = self.commit("Another event")
        self.publish_tag("v2.9.1", other)
        self.clone_worker()
        tags = self.remote_tags()

        with self.assertRaisesRegex(ValueError, "v2.9.1 already tags another commit"):
            self.tag_push(self.initial, after)
        self.assertEqual(tags, self.remote_tags())
        self.assert_remote_tag("v2.9.1", other)

    def test_conflicting_local_tag_is_never_overwritten(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        other = self.commit("Local-only commit", repository=self.worker)
        self.git(self.worker, "tag", "v2.9.1", other)
        tags = self.remote_tags()

        with self.assertRaises(subprocess.CalledProcessError):
            self.tag_push(self.initial, after)
        self.assertEqual(tags, self.remote_tags())
        self.assertEqual(
            other, self.git(self.worker, "rev-parse", "refs/tags/v2.9.1")
        )

    def test_same_event_racing_push_is_idempotent(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        real_git = tag_release_branch.git
        competing_tags = []

        def race(*args):
            if args[0] == "push":
                # Publish a different annotated tag object at the same SHA,
                # ensuring the implementation's real push is rejected.
                self.publish_tag("v2.9.1", after)
                competing_tags.append(self.remote_tags())
            return real_git(*args)

        with mock.patch.object(tag_release_branch, "git", side_effect=race):
            self.assertEqual("v2.9.1", self.tag_push(self.initial, after))
        self.assertEqual([self.remote_tags()], competing_tags)
        self.assert_remote_tag("v2.9.1", after)

    def test_conflicting_racing_push_is_never_overwritten(self):
        after = self.commit("Release fix")
        self.push_branch()
        other = self.commit("Another event")
        self.clone_worker()
        real_git = tag_release_branch.git
        competing_tags = []

        def race(*args):
            if args[0] == "push":
                self.publish_tag("v2.9.1", other)
                competing_tags.append(self.remote_tags())
            return real_git(*args)

        with mock.patch.object(tag_release_branch, "git", side_effect=race):
            with self.assertRaises(subprocess.CalledProcessError) as error:
                self.tag_push(self.initial, after)
        self.assertEqual("push", error.exception.cmd[1])
        self.assertEqual([self.remote_tags()], competing_tags)
        self.assert_remote_tag("v2.9.1", other)

    def test_accepts_existing_lightweight_tag_at_event_sha(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.publish_tag("v2.9.1", after, annotated=False)
        self.clone_worker()
        tags = self.remote_tags()

        self.assertEqual("v2.9.1", self.tag_push(self.initial, after))
        self.assertEqual(tags, self.remote_tags())

    def test_lightweight_prior_head_tag_is_supported(self):
        self.git(self.writer, "tag", "-d", "v2.9.0")
        self.git(self.writer, "push", "origin", ":refs/tags/v2.9.0")
        self.publish_tag("v2.9.4", self.initial, annotated=False)
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()

        self.assertEqual("v2.9.5", self.tag_push(self.initial, after))
        self.assert_remote_tag("v2.9.5", after)

    def test_invalid_shas_fail_without_changing_tags(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        tags = self.remote_tags()
        for invalid in ("", "0" * 39, "0" * 41, "g" * 40, "A" * 40, "HEAD"):
            for before, event in ((invalid, after), (self.initial, invalid)):
                with self.subTest(before=before, after=event):
                    with self.assertRaisesRegex(ValueError, "full before and after"):
                        self.tag_push(before, event)
        self.assertEqual(tags, self.remote_tags())

    def test_same_before_and_after_fails(self):
        self.clone_worker()
        tags = self.remote_tags()
        with self.assertRaisesRegex(ValueError, "push with new commits"):
            self.tag_push(self.initial, self.initial)
        self.assertEqual(tags, self.remote_tags())

    def test_non_fast_forward_fails(self):
        before = self.commit("Old branch head")
        self.push_branch()
        self.git(self.writer, "checkout", "-b", "replacement", self.initial)
        after = self.commit("Divergent branch head")
        self.git(
            self.writer,
            "push",
            "--force",
            "origin",
            "replacement:refs/heads/bb_release_1",
        )
        self.clone_worker()
        # A real force-push checkout may still have the old event commit.
        self.git(self.worker, "fetch", str(self.writer), before)
        tags = self.remote_tags()
        with self.assertRaises(subprocess.CalledProcessError) as error:
            self.tag_push(before, after)
        self.assertEqual(1, error.exception.returncode)
        self.assertEqual("merge-base", error.exception.cmd[1])
        self.assertEqual(tags, self.remote_tags())

    def test_unknown_commit_fails(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        tags = self.remote_tags()
        with self.assertRaises(subprocess.CalledProcessError):
            self.tag_push("0" * 40, after)
        self.assertEqual(tags, self.remote_tags())

    def test_shallow_clone_fails(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker(shallow=True)
        self.assertEqual(
            "true", self.git(self.worker, "rev-parse", "--is-shallow-repository")
        )
        tags = self.remote_tags()
        with self.assertRaisesRegex(ValueError, "Full history is required"):
            self.tag_push(self.initial, after)
        self.assertEqual(tags, self.remote_tags())


if __name__ == "__main__":
    unittest.main()
