import json
import os
import re
import subprocess
import sys
import tempfile

AUTO_RUN_GAZELLE_PREFERENCE_KEY = "autoRunGazelle"


def main():
    # If not running in an interactive terminal session, don't do anything.
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return

    bazel_output_path = sys.argv[1]
    with open(bazel_output_path, "r") as f:
        lines = f.readlines()
    packages = set()
    problems = []
    for line in lines:
        if ".go" not in line and "import of" not in line:
            continue
        m = re.search('^.*/execroot/.*?/(.*?\.go): import of "(.*?)"', line)
        if not m:
            continue
        problems.append(
            {
                "src_path": m.group(1),
                "import_url": m.group(2),
            }
        )

    if not problems:
        return

    packages = set([get_package(problem["src_path"]) for problem in problems])

    autorun = get_preference(AUTO_RUN_GAZELLE_PREFERENCE_KEY)
    if autorun is None:
        autorun, dont_ask_again = prompt("Run gazelle to fix these packages?")
        if dont_ask_again:
            set_preference(AUTO_RUN_GAZELLE_PREFERENCE_KEY, autorun)
    if not autorun:
        return

    build_workspace_directory = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")
    if not build_workspace_directory:
        print("$BUILD_WORKSPACE_DIRECTORY is not set; exiting.", file=sys.stderr)
        return 1

    with tempfile.NamedTemporaryFile() as run_script:
        print(
            "\x1b[90m> bazel run //:gazelle -- "
            + "".join(packages)
            + "\x1b[m  🛠️  fixing...",
            end=""
        )
        sys.stdout.flush()
        p = subprocess.run(
            [
                "bazel",
                "run",
                "--script_path=" + run_script.name,
                "//:gazelle",
                "--",
                *packages,
            ],
            check=False,
            cwd=build_workspace_directory,
            capture_output=True,
        )
        if p.returncode != 0:
            erase_current_line()
            print(
                "\x1b[32m> bazel run //:gazelle -- "
                + "".join(packages)
                + "\x1b[m  ❌ fix failed"
            )
            print(p.stderr, file=sys.stderr)
            return p.returncode
        os.chmod(run_script.name, 0o755)
        p = subprocess.run(["/usr/bin/env", "bash", run_script.name], check=False)
        if p.returncode != 0:
            return p.returncode
        # TODO(bduffany): Retry the build up to one time once the fix succeeds.
        erase_current_line()
        print(
            "\x1b[32m> bazel run //:gazelle -- "
            + "".join(packages)
            + "\x1b[m  ✅ fix applied"
        )


def get_package(relative_src_path):
    return os.path.dirname(relative_src_path)


def prompt(msg):
    while True:
        print("\x1b[36m> " + msg + "\x1b[0;90m (yes)/always/no/never: \x1b[m", end="")
        response = input().lower().strip()
        # Return (response, dont_ask_again) pair
        if response in ("", "yes", "y"):
            return True, False
        if response == "always":
            return True, True
        if response in ("no", "n"):
            return False, False
        if response == "never":
            return False, True
        print("\x1b[31mInvalid response.\x1b[m")


def erase_current_line():
    print('\x1b[2K\r', end="")

# TODO: Have the CLI provide a more standard preference system
def preferences_path():
    # Note: USER_CONFIG_DIR should be set by the CLI.
    user_config_dir = os.environ.get("USER_CONFIG_DIR")
    if not user_config_dir:
        print("$USER_CONFIG_DIR is not set; exiting.", file=sys.stderr)
        exit(1)
    return os.path.join(user_config_dir, "bb-go-deps-plugin", "preferences.json")


def read_preferences():
    if not os.path.exists(preferences_path()):
        return {}
    with open(preferences_path(), "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print(
                "Invalid JSON found in preferences file at " + preferences_path(),
                file=sys.stderr,
            )
            exit(1)


def get_preference(key, default_value=None):
    return read_preferences().get(key, default_value)


def set_preference(key, value):
    preferences = read_preferences()
    preferences[key] = value
    os.makedirs(os.path.dirname(preferences_path()), exist_ok=True)
    with open(preferences_path(), "w") as f:
        json.dump(preferences, f)


if __name__ == "__main__":
    exit(main())
