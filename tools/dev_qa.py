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
API_KEY = os.environ.get(
    "BB_API_KEY",
)

# NOTE: If you change these commit_shas, be sure to update dev-qa.yaml in
# buildbuddy-internal
REPO_CONFIGS = [
    {
        "name": "buildbuddy",
        "repo_url": "https://github.com/buildbuddy-io/buildbuddy",
        "commit_sha": "3d001c946273dca873086b58170f35cfb0c06cbb",
        "command": """
            bazel test //... \
                --config=remote-dev \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --flaky_test_attempts=3 \
                --build_metadata=TAGS=dev-qa \
                --remote_header=x-buildbuddy-api-key={}
        """.format(API_KEY),
    },
    {
        "name": "bazel-gazelle",
        "repo_url": "https://github.com/bazelbuild/bazel-gazelle",
        "commit_sha": "f44f85943a3f6bde872e2d39c5b552e21a797975",
        "command": """
            bazel build //... \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --remote_timeout=10m \
                --build_metadata=TAGS=dev-qa \
                --noenable_bzlmod \
                --extra_execution_platforms=@buildbuddy_toolchain//:platform \
                --host_platform=@buildbuddy_toolchain//:platform \
                --platforms=@buildbuddy_toolchain//:platform \
                --crosstool_top=@buildbuddy_toolchain//:toolchain \
                --remote_header=x-buildbuddy-api-key={}
        """.format(API_KEY),
    },
    {
        "name": "abseil-cpp",
        "repo_url": "https://github.com/abseil/abseil-cpp",
        "commit_sha": "e968256406fd7898d7fde880e31e54b041d32a7e",
        "command": """
            bazel build //... \
                --cxxopt="-std=c++14" \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --remote_timeout=10m \
                --build_metadata=TAGS=dev-qa \
                --noenable_bzlmod \
                --extra_execution_platforms=@buildbuddy_toolchain//:platform \
                --host_platform=@buildbuddy_toolchain//:platform \
                --platforms=@buildbuddy_toolchain//:platform \
                --crosstool_top=@buildbuddy_toolchain//:toolchain \
                --remote_header=x-buildbuddy-api-key={}
        """.format(API_KEY),
    },
    {
        "name": "rules_python",
        "repo_url": "https://github.com/bazelbuild/rules_python",
        "commit_sha": "da10ac49efee1b02cbfa3b22a39e68bf3fe5bbe2",
        "command": """
            bazel build //... \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --remote_timeout=10m \
                --jobs=100 \
                --build_metadata=TAGS=dev-qa \
                --extra_execution_platforms=@buildbuddy_toolchain//:platform \
                --host_platform=@buildbuddy_toolchain//:platform \
                --platforms=@buildbuddy_toolchain//:platform \
                --crosstool_top=@buildbuddy_toolchain//:toolchain \
                --remote_header=x-buildbuddy-api-key={}
        """.format(API_KEY),
    },
    {
        "name": "tensorflow",
        "repo_url": "https://github.com/tensorflow/tensorflow",
        "commit_sha": "df75ddb32a31ba79b58679d833dfa1af478d04a8",
        "command": """
            bazel build tensorflow \
                --config=rbe_linux_cpu \
                --config=monolithic \
                --remote_executor=remote.buildbuddy.dev \
                --remote_cache=remote.buildbuddy.dev \
                --bes_backend=remote.buildbuddy.dev \
                --bes_results_url=https://app.buildbuddy.dev/invocation/ \
                --nogoogle_default_credentials \
                --build_metadata=TAGS=dev-qa \
                --remote_header=x-buildbuddy-api-key={}
        """.format(API_KEY),
    },
]

BUILDBUDDY_TOOLCHAIN_SNIPPET = """
http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "e8ba5cf78c8a6268a08cf563c54d3d23a7edf288a16b39fadc8b8a27b2527155",
    strip_prefix = "buildbuddy-toolchain-f52e991c46e4bb6c71320db3970c20ce088ce951",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/f52e991c46e4bb6c71320db3970c20ce088ce951.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")
buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy", "UBUNTU20_04_IMAGE")
buildbuddy(name = "buildbuddy_toolchain", container_image = UBUNTU20_04_IMAGE)
"""

def run_test(name, repo_url, commit_sha, command, clean_repos=False):
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
        git checkout {commit_sha}

        # Add buildbuddy rbe toolchain to WORKSPACE if it's not already in there
        if ! grep -q "io_buildbuddy_buildbuddy_toolchain" "WORKSPACE"; then
            echo '{BUILDBUDDY_TOOLCHAIN_SNIPPET}' >> WORKSPACE
        fi

        # Pin to a specific bazel version
        echo '7.0.2' > .bazelversion

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
        default="buildbuddy,bazel-gazelle,abseil-cpp,rules_python",
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
    success = True
    for repo in repos:
        (exit_code, invocation_url) = run_test(
            repo["name"],
            repo["repo_url"],
            repo["commit_sha"],
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
        if exit_code != 0:
            success = False
            if not args.keep_going:
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

    if not success:
        sys.exit(1)
