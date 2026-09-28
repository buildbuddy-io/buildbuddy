"""Real-Git release.py fixtures; all origins are temporary local bare repos."""

import contextlib
import importlib.util
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
from unittest import mock


class ReleaseRepository:
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=os.environ.get("TEST_TMPDIR"))
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        release_path = Path(__file__).resolve().parent.parent / "release.py"
        spec = importlib.util.spec_from_file_location("release_under_test", release_path)
        self.release = importlib.util.module_from_spec(spec)
        self.requests = types.ModuleType("requests")
        self.requests.get = mock.Mock(
            side_effect=AssertionError("Unexpected HTTP request")
        )
        with mock.patch.dict(sys.modules, {"requests": self.requests}):
            spec.loader.exec_module(self.release)
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
        self.publish_tag(
            "v2.9.0", self.initial,
            message="Initial release\n\nRelease-Branch: bb_release_1",
        )
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

    def patch_release(
        self, previous_commit=None, repository=None, branch="bb_release_1",
        attempts=1, interval=7,
    ):
        with self.in_repository(repository or self.worker):
            old_version = self.release.get_patch_base_version(
                branch, previous_commit=previous_commit,
                attempts=attempts, interval=interval,
            )
            version = self.release.bump_patch_version(old_version)
            self.release.create_and_push_tag(old_version, version, branch=branch)
            return version

    def create_tag(self, old_version="v2.9.0", new_version="v2.9.1", **kwargs):
        with self.in_repository(self.worker):
            return self.release.create_and_push_tag(old_version, new_version, **kwargs)

    def run_main(self, *args, repository=None):
        stdout = io.StringIO()
        with (
            self.in_repository(repository or self.worker),
            mock.patch.object(sys, "argv", ["release.py", *args]),
            mock.patch.dict(
                os.environ, {"GITHUB_OUTPUT": str(self.root / "outputs")}
            ),
            contextlib.redirect_stdout(stdout),
            mock.patch.object(
                self.release.time, "sleep",
                side_effect=AssertionError("Unexpected wait"),
            ),
        ):
            self.release.main()
        return stdout.getvalue()

    def replace_initial_tag(self, annotated=True, message=None):
        self.git(self.writer, "tag", "-d", "v2.9.0")
        self.git(self.writer, "push", "origin", ":refs/tags/v2.9.0")
        self.publish_tag("v2.9.0", self.initial, annotated=annotated, message=message)

    def prepare_patch(self):
        after = self.commit("Release fix")
        self.push_branch()
        self.clone_worker()
        return after

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
