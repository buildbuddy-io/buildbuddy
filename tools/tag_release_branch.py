#!/usr/bin/env python3
"""Tag a release-branch push, using the version at its previous head.

The internal cut-release workflow owns the initial minor tag. Each subsequent
push gets one patch tag at its exact event SHA (not the moving branch head).
Waiting for the previous head's tag handles overlapping pushes and the window
between branch creation and its initial tag. Missing tags fail closed: rerun
after repairing the earlier run rather than guessing a version from master.
"""

import argparse
import re
import subprocess
import time


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def remote_tag_commit(tag):
    refs = dict(
        line.split()[::-1]
        for line in git(
            "ls-remote", "origin", f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}"
        ).splitlines()
    )
    return refs.get(f"refs/tags/{tag}^{{}}", refs.get(f"refs/tags/{tag}"))


def versions_at_commit(commit, branch):
    versions = []
    owned_versions = []
    for tag in git("tag", "--points-at", commit, "-l", "v*").splitlines():
        if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag):
            continue
        version = tuple(map(int, tag[1:].split(".")))
        # For lightweight tags, %(contents) is the commit message, not tag
        # metadata. Never interpret a commit's trailer as tag ownership.
        contents = ""
        if git("cat-file", "-t", f"refs/tags/{tag}") == "tag":
            contents = git("for-each-ref", "--format=%(contents)", f"refs/tags/{tag}")
        last_line = contents.splitlines()[-1] if contents else ""
        owner = (
            last_line.removeprefix("Release-Branch: ")
            if last_line.startswith("Release-Branch: ")
            else None
        )
        versions.append((version, owner))
        if branch and owner == branch:
            owned_versions.append(version)
    if owned_versions:
        return owned_versions
    if branch:
        # Even a lone legacy tag might belong to an older branch cut at the
        # same commit. Wait for this branch's tag rather than guessing.
        return []
    return [version for version, _ in versions]


def tag_push(before, after, attempts=90, interval=10, branch=None):
    if not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (before, after)):
        raise ValueError("Expected full before and after commit SHAs")
    if git("rev-parse", "--is-shallow-repository") == "true":
        raise ValueError("Full history is required")
    if before == after:
        raise ValueError("Expected a push with new commits")
    if branch is not None and not re.fullmatch(r"bb_release_[A-Za-z0-9_-]+", branch):
        raise ValueError("Expected a bb_release_* branch")
    git("merge-base", "--is-ancestor", before, after)

    for attempt in range(attempts):
        git("fetch", "origin", "--tags")
        versions = versions_at_commit(before, branch)
        if versions:
            break
        if attempt + 1 < attempts:
            print(f"Waiting for the release tag at previous head {before}", flush=True)
            time.sleep(interval)
    else:
        raise ValueError(
            f"No release version tag at {before} for branch {branch}; repair the initial cut or "
            "previous push's tagging run, then rerun this workflow"
        )

    if len({version[:2] for version in versions}) != 1:
        raise ValueError(f"Ambiguous release version tags at {before}")
    major, minor, patch = max(versions)
    tag = f"v{major}.{minor}.{patch + 1}"
    existing = remote_tag_commit(tag)
    if existing == after:
        print(f"{tag} already tags {after}")
        return tag
    if existing:
        raise ValueError(f"{tag} already tags another commit: {existing}")

    message = f"Release {tag}"
    if branch:
        message += f"\n\nRelease-Branch: {branch}"
    git("tag", "-a", tag, after, "-m", message)
    try:
        git("push", "origin", f"refs/tags/{tag}:refs/tags/{tag}")
    except subprocess.CalledProcessError:
        # Two runs of the same event may race. Never overwrite a tag, but accept
        # an identical result from the other run.
        if remote_tag_commit(tag) != after:
            raise
    print(f"Tagged {after} as {tag}")
    return tag


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before")
    parser.add_argument("after")
    parser.add_argument("branch")
    args = parser.parse_args()
    tag_push(args.before, args.after, branch=args.branch)


if __name__ == "__main__":
    main()
