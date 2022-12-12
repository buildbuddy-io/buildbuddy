#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "version",
        nargs="?",
        default="",
        help="Version to release, like '1.2.3'. By default, bumps the current patch version.",
    )
    args = parser.parse_args()
    exit(_main(**vars(args)))


def _main(version):
    # Fetch existing versions
    print("> Fetching tags...")
    p = sh("git fetch --tags")

    # Get latest tag
    p = sh("git tag -l 'cli-*' --sort=creatordate | tail -n1", stream_stdout=False)
    latest_tag = p.stdout.strip()

    # If version is unset, bump the latest patch version
    if not version:
        latest_version = strip_prefix(latest_tag, "cli-v")
        major, minor, patch = [int(part) for part in latest_version.split(".")]
        version = "%d.%d.%d" % (major, minor, patch + 1)

    # Normalize input versions like "cli-vX.Y.Z" or "vX.Y.Z" to just "X.Y.Z"
    version = strip_prefix(version, "cli-")
    version = strip_prefix(version, "v")

    try:
        (_major, _minor, _patch) = [int(part) for part in version.split(".")]
    except:
        print('error: invalid version format "%s" (expected X.Y.Z)' % version)
        return 1

    tag = "cli-v" + version

    # Check whether tag already exists
    p = sh("git tag -l '%s' || true" % tag, stream_stdout=False)
    if p.stdout:
        print("error: tag '%s' already exists" % tag)
        return 1

    # Print latest tag, for comparison
    print("> Latest tag:      %s" % latest_tag)
    print("> Confirm new tag: %s (Enter = OK, Ctrl+C = cancel)" % tag)
    try:
        input()
    except KeyboardInterrupt:
        print()
        return 1

    # Tag new version
    sh("git tag " + tag)
    sh("git push origin " + tag)

    print("---")
    print("> Tag pushed!")

    print("> Release workflow should show up here:")
    print(
        "> https://github.com/buildbuddy-io/buildbuddy/actions/workflows/release-cli.yaml"
    )

    print("> Once the workflow has succeeded, publish the draft release here:")
    print("> https://github.com/buildbuddy-io/bazel/releases")

    print("---")
    print("> Run cli/update_docs.sh to update website docs? (Enter=OK, Cancel=Ctrl+C)")
    try:
        input()
    except KeyboardInterrupt:
        print()
        return 1
    subprocess.run(["cli/update_docs.sh", version])


def sh(command, stream_stdout=True, **kwargs):
    p = subprocess.Popen(
        ["bash", "-e", "-c", command],
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        encoding="utf-8",
        **kwargs
    )
    stdout = ""
    for line in p.stdout:
        if stream_stdout:
            print(line, end="")
        stdout += line
    p.wait()
    if p.returncode != 0:
        exit(p.returncode)
    p.stdout = stdout
    return p


def strip_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


if __name__ == "__main__":
    main()
