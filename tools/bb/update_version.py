#!/usr/bin/env python3
"""Updates the prebuilt bb CLI version used by //tools/lint."""

import base64
import json
import os
import pathlib
import re
import sys
import urllib.request

PLATFORMS = [
    "darwin-arm64",
    "darwin-x86_64",
    "linux-arm64",
    "linux-x86_64",
]


def fetch_text(url: str) -> str:
    with urllib.request.urlopen(url) as response:
        return response.read().decode("utf-8")


def get_latest_version() -> str:
    release = json.loads(fetch_text("https://api.github.com/repos/buildbuddy-io/bazel/releases/latest"))
    version = release.get("tag_name", "")
    if not version:
        raise SystemExit("Failed to determine latest release")
    print(f"Latest release: {version}")
    return version


def sha256_integrity(version: str, platform: str) -> str:
    url = f"https://github.com/buildbuddy-io/bazel/releases/download/{version}/bazel-{version}-{platform}.sha256"
    hex_digest = fetch_text(url).strip().split()[0]
    if not hex_digest:
        raise SystemExit(f"Failed to fetch sha256 for {platform}")
    integrity = "sha256-" + base64.b64encode(bytes.fromhex(hex_digest)).decode("ascii")
    print(f"{platform}: {integrity}")
    return integrity


def update_deps_bzl(path: pathlib.Path, version: str, sums: dict[str, str]) -> None:
    content = path.read_text()
    content, n = re.subn(
        r'^BB_CLI_VERSION = ".*"$',
        f'BB_CLI_VERSION = "{version}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    if n != 1:
        raise SystemExit("failed to update BB_CLI_VERSION")

    for platform, integrity in sums.items():
        repo_name = f"io_buildbuddy_bb_cli-{platform}"
        pattern = rf'(name = "{re.escape(repo_name)}",.*?\n\s*executable = True,\n\s*integrity = ")sha256-[^"]*(")'
        content, n = re.subn(pattern, rf'\1{integrity}\2', content, count=1, flags=re.DOTALL)
        if n != 1:
            raise SystemExit(f"failed to update integrity for {repo_name}")

    path.write_text(content)


def main() -> None:
    version = sys.argv[1] if len(sys.argv) >= 2 else get_latest_version()
    workspace_dir = pathlib.Path(
        os.environ.get("BUILD_WORKSPACE_DIRECTORY", pathlib.Path(__file__).resolve().parents[2])
    )
    deps_bzl = workspace_dir / "deps.bzl"

    sums = {platform: sha256_integrity(version, platform) for platform in PLATFORMS}
    update_deps_bzl(deps_bzl, version, sums)

    print()
    print(f"Updated deps.bzl to bb CLI version {version}")


if __name__ == "__main__":
    main()
