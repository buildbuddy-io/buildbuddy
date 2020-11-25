"""This script runs a bunch of builds against BuildBuddy in a loop."""

import os
import random
import subprocess
import sys
import time
import multiprocessing

# This value can be increased depending on the local machine's specs.
# The more parallel builds, the better the simulation.
MAX_PARALLEL_INVOCATIONS = 2

BAZEL_CONFIG = "local"


def sh(cmd):
    return subprocess.run(cmd, shell=True)


def sh_get_list(cmd):
    stdout = subprocess.run(cmd, shell=True, capture_output=True).stdout
    return all_except_empty(stdout.decode("utf-8").splitlines())


def all_except_empty(lines):
    return [line for line in lines if line]


def random_file(extensions):
    all_paths = sh_get_list("find . -type f")
    candidates = [
        file_path
        for file_path in all_paths
        if any((file_path.endswith(extension) for extension in extensions))
    ]
    return random.choice(candidates)


def random_code_char():
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789() \n"
    return random.choice(chars)


bazel_lock = multiprocessing.BoundedSemaphore(MAX_PARALLEL_INVOCATIONS)


class Developer:
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def develop(self):
        self.init_repo()
        while True:
            self.maybe_make_edits()
            self.build()
            time.sleep(random.randint(10, 30))
            self.maybe_scrap_all_edits()

    def build(self):
        with bazel_lock:
            sh(f"bazel build //server --config={BAZEL_CONFIG}")

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
    os.chdir(sys.path[0])

    data_dir = "/tmp/tmp_buildbuddy_simulate_traffic"
    print(f"Data directory: {data_dir}")

    if not os.path.exists(data_dir):
        sh(f"mkdir -p {data_dir}")
    os.chdir(data_dir)
    if not os.path.exists("buildbuddy"):
        sh("git clone https://github.com/buildbuddy-io/buildbuddy")

    def spawn_developer(data_dir):
        Developer(data_dir).develop()

    num_developers = 5
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
