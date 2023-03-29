#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
import uuid

USAGE = """tools/dev_qa.py [--repos=buildbuddy,bazel-gazelle,...]

Runs QA builds against BuildBuddy.

Example:
    # Test the default set of repos
    tools/dev_qa.py

    # Test all repos
    tools/dev_qa.py --repos=all

    # Test a subset of repos
    tools/dev_qa.py --repos=buildbuddy,bazel-gazelle
"""

QA_ROOT = os.environ.get(
    "QA_ROOT", os.path.join(os.path.expanduser("~"), "buildbuddy-qa")
)

REPO_CONFIGS = [
    {
        "name": "buildbuddy",
        "repo_url": "https://github.com/buildbuddy-io/buildbuddy",
        "command": """
            bazel test //... \
                --config=remote-dev \
                --flaky_test_attempts=3
        """,
    },
    {
        "name": "bazel-gazelle",
        "repo_url": "https://github.com/bazelbuild/bazel-gazelle",
        "command": """
            bazel build //... \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --remote_timeout=10m
        """,
    },
    {
        "name": "abseil-cpp",
        "repo_url": "https://github.com/abseil/abseil-cpp",
        "command": """
            bazel build //... \
                --cxxopt="-std=c++14" \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --remote_timeout=10m \
                --extra_execution_platforms=@buildbuddy_toolchain//:platform \
                --host_platform=@buildbuddy_toolchain//:platform \
                --platforms=@buildbuddy_toolchain//:platform \
                --crosstool_top=@buildbuddy_toolchain//:toolchain
        """,
    },
    {
        "name": "bazel",
        "repo_url": "https://github.com/bazelbuild/bazel",
        "command": """
            bazel build //src/main/java/com/google/devtools/build/lib/... \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --remote_timeout=10m \
                --extra_execution_platforms=@buildbuddy_toolchain//:platform \
                --host_platform=@buildbuddy_toolchain//:platform \
                --platforms=@buildbuddy_toolchain//:platform \
                --crosstool_top=@buildbuddy_toolchain//:toolchain \
                --noincompatible_disallow_empty_glob \
                --java_runtime_version=remotejdk_17 \
                --jobs=100
        """,
    },
    {
        "name": "rules_python",
        "repo_url": "https://github.com/bazelbuild/rules_python",
        "command": """
            bazel build //... \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --remote_timeout=10m \
                --jobs=100
        """,
    },
    {
        "name": "tensorflow",
        "repo_url": "https://github.com/tensorflow/tensorflow",
        "command": """
            bazel build tensorflow \
                --config=rbe_cpu_linux \
                --config=monolithic \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --nogoogle_default_credentials
        """,
    },
]

BUILDBUDDY_TOOLCHAIN_SNIPPET = """
http_archive (
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "e899f235b36cb901b678bd6f55c1229df23fcbc7921ac7a3585d29bff2bf9cfd",
    strip_prefix = "buildbuddy-toolchain-fd351ca8f152d66fc97f9d98009e0ae000854e8f",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/fd351ca8f152d66fc97f9d98009e0ae000854e8f.tar.gz"],
)
load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")
buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy", "UBUNTU20_04_IMAGE")
buildbuddy(name = "buildbuddy_toolchain", container_image = UBUNTU20_04_IMAGE)
"""


def run_test(name, repo_url, command, clean_repos=False):
    command = " ".join(command.split())
    invocation_id = str(uuid.uuid4())

    script = f"""
        clone_dir="$PWD/{name}"
        cleanup() {{
            (({int(clean_repos)})) && rm -rf "$clone_dir"
        }}
        cleanup && trap cleanup EXIT

        ! [[ -e ./{name} ]] && git clone {repo_url} {name}
        cd ./{name}

        # Add buildbuddy rbe toolchain to WORKSPACE if it's not already in there
        if ! grep -q "io_buildbuddy_buildbuddy_toolchain" "WORKSPACE" "WORKSPACE.bazel"; then
            echo '{BUILDBUDDY_TOOLCHAIN_SNIPPET}' >> WORKSPACE
        fi
        set -x
        bazel clean
        {command} --invocation_id={invocation_id}
    """
    p = subprocess.run(
        ["bash", "-e", "-c", script],
        cwd=QA_ROOT,
        stdout=sys.stdout,
        stderr=sys.stderr,
        check=False,
    )

    invocation_link = f"https://app.buildbuddy.dev/invocation/{invocation_id}"
    return (p.returncode, invocation_link)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(usage=USAGE)
    parser.add_argument(
        "--repos",
        default="buildbuddy,bazel-gazelle,abseil-cpp,bazel,rules_python",
        help="Which repos to test.",
    )
    parser.add_argument(
        "-c",
        "--clean_repos",
        default=False,
        action="store_true",
        help="Whether to freshly clone the repo before the build and delete it afterwards.",
    )
    parser.add_argument(
        "--nokeep_going",
        dest="keep_going",
        action="store_false",
        help="Don't continue if a command fails.",
    )
    args = parser.parse_args()

    if args.repos == "all":
        repos = REPO_CONFIGS
    else:
        repos_by_name = {c["name"]: c for c in REPO_CONFIGS}
        repos = [repos_by_name[name] for name in args.repos.split(",")]

    os.makedirs(QA_ROOT, exist_ok=True)

    results = []
    for repo in repos:
        (exit_code, invocation_url) = run_test(
            repo["name"],
            repo["repo_url"],
            repo["command"],
            clean_repos=args.clean_repos,
        )
        results.append(
            {
                "name": repo["name"],
                "exit_code": exit_code,
                "invocation_url": invocation_url,
            }
        )
        if exit_code != 0 and not args.keep_going:
            break

    if len(results) < len(repos):
        missing = repos[len(results) :]
        for repo in missing:
            results.append({"name": repo["name"]})

    print("---")
    print("Results:")
    for result in results:
        if "exit_code" not in result:
            print(f"- {result['name']}: NO STATUS")
            continue

        status = "SUCCESS"
        if result["exit_code"] != 0:
            status = f"FAILURE (exit code {exit_code})"
        print(f"- {result['name']}: {status} ({result['invocation_url']})")
