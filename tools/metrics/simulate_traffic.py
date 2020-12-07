#!/usr/bin/env python3
"""This script runs a bunch of builds against BuildBuddy in a loop."""

import argparse
import multiprocessing
import os
import random
import subprocess
import sys
import time


def sh(cmd):
    return subprocess.run(cmd, shell=True)


def sh_get_list(cmd):
    stdout = subprocess.run(cmd, shell=True, capture_output=True).stdout
    return get_nonblank(stdout.decode("utf-8").splitlines())


def get_nonblank(lines):
    return [line for line in (line.strip() for line in lines) if line]


def random_file(extensions):
    all_paths = sh_get_list("find . -type f")
    return random.choice(
        [
            file_path
            for file_path in all_paths
            if any((file_path.endswith(extension) for extension in extensions))
        ]
    )


def random_code_char():
    return random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789() \n")


class Developer:
    def __init__(self, data_dir, bazel_configs, bazel_lock):
        self.bazel_lock = bazel_lock
        self.bazel_configs = bazel_configs
        self.data_dir = data_dir

    def develop(self):
        self.init_repo()
        while True:
            self.maybe_make_edits()
            self.build()
            time.sleep(random.randint(10, 30))
            self.maybe_scrap_all_edits()

    def build(self):
        with self.bazel_lock:
            config_flags = " ".join(
                [f"--config={config}" for config in self.bazel_configs or ["local"]]
            )
            sh(f"bazel build //server {config_flags}")

    def init_repo(self):
        sh(f"mkdir -p {self.data_dir}")
        os.chdir(self.data_dir)
        if not os.path.exists("buildbuddy"):
            sh("cp -R ../buildbuddy ./")
        os.chdir("buildbuddy")
        sh("git reset --hard HEAD")
        sh("git pull")

    def maybe_make_edits(self):
        if random.random() < 0.3:
            return

        file_path = random_file([".tsx", ".go", ".css"])
        with open(file_path, "r") as f:
            content = f.read()
        i = random.randint(0, len(content) - 1)
        content = content[:i] + random_code_char() + content[i + 1 :]
        with open(file_path, "w") as f:
            f.write(content)

    def maybe_scrap_all_edits(self):
        if random.random() < 0.2:
            return
        sh("git reset --hard HEAD")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--bazel-config",
        help=(
            "Bazel --config flag values to use. "
            "Configs are sourced from the .bazelrc defined in the BuildBuddy repo at HEAD (on GitHub) "
            "as well as the local ~/.bazelrc."
        ),
        action="append",
    )
    parser.add_argument(
        "-p",
        "--max-parallel-invocations",
        help="Max number of parallel bazel invocations.",
        type=int,
        default=2,
    )
    parser.add_argument(
        "-n",
        "--num-developers",
        help="Number of developers in the simulation.",
        type=int,
        default=5,
    )
    args = parser.parse_args()

    os.chdir(sys.path[0])

    data_dir = "/tmp/tmp_buildbuddy_simulate_traffic"
    print(f"Data directory: {data_dir}")

    if not os.path.exists(data_dir):
        sh(f"mkdir -p {data_dir}")
    os.chdir(data_dir)
    if not os.path.exists("buildbuddy"):
        sh("git clone https://github.com/buildbuddy-io/buildbuddy")

    bazel_lock = multiprocessing.BoundedSemaphore(args.max_parallel_invocations)
    bazel_configs = args.bazel_config

    def spawn_developer(data_dir):
        Developer(data_dir, bazel_configs, bazel_lock).develop()

    num_developers = args.num_developers
    procs = []
    for developer_id in range(1, num_developers + 1):
        developer_data_dir = os.path.join(data_dir, f"developer_{developer_id}")
        p = multiprocessing.Process(target=spawn_developer, args=(developer_data_dir,))
        procs.append(p)
        p.start()
    for p in procs:
        p.join()


if __name__ == "__main__":
    main()
