#!/bin/bash
# Runs prettier on all files that differ from the main branch.

set -e

# Replacement for GNU realpath (not available on Mac)
realpath() {
  [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

# Diagram showing what `git merge-base HEAD refs/remotes/origin/master` is doing:
#
# o <- fetched remote master branch (refs/remotes/origin/master)
# |
# o  o <- current local branch (HEAD)
# | /
# o <- $(git merge-base HEAD refs/remotes/origin/master)
# |
#
# We're finding the latest common ancestor between the remote master branch
# and the current branch, so that we only run the lint check on files added
# in this branch.

function paths_to_format() {
  git merge-base HEAD refs/remotes/origin/master |
    xargs git diff --name-only --diff-filter=AMRCT |
    while read -r path; do
      if [[ "$path" =~ \.(js|jsx|ts|tsx|html|css|yaml|json|md|xml)$ ]]; then
        echo "$path"
      fi
    done
}

paths=()
while read -r path; do
  paths+=("$path")
done < <(paths_to_format)

if [[ -z "${paths[*]}" ]]; then
  exit 0
fi

# Run bazel quietly; see: https://github.com/bazelbuild/bazel/issues/4867#issuecomment-796208829
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
bazel run @npm//prettier/bin:prettier --script_path="$tmp/run.sh" &>"$tmp/build.log" || {
  cat "$tmp/build.log" >&2
  exit 1
}
chmod +x "$tmp/run.sh"
"$tmp/run.sh" --bazel_node_working_dir="$PWD" "${paths[@]}" "$@"
