#!/usr/bin/env bash
set -euo pipefail

DEFAULT_KYTHE_DOWNLOAD_URL="https://storage.googleapis.com/buildbuddy-tools/archives/kythe-v0.0.78-buildbuddy.tar.gz"

usage() {
  cat <<'EOF'
Run a local Kythe indexing flow mirroring the "Generate Kythe Annotations" workflow.

Usage:
  bazel run //tools/kythe:local_kythe -- [options] [repo_root]

Options:
  --repo_root <path>    Repository root to index (default: current directory)
  --out_path <path>     Output sstable path (default: <repo_root>/kythe_serving.sst)
  --skip_cxx            Skip C++ indexing and memcached setup
  --bazel_version <v>   Set USE_BAZEL_VERSION for nested bazel commands
  --distdir <path>      Bazel distdir to avoid flaky external fetches
  --scope <mode>        Build scope: proto|default|full (default: default)
  --dry_run             Print planned commands/settings without executing
  -h, --help            Show this help

Env vars:
  KYTHE_DOWNLOAD_URL    Kythe archive URL
  OUT_PATH              Same as --out_path
  SKIP_CXX=1            Same as --skip_cxx
  USE_BAZEL_VERSION     Same as --bazel_version
  BAZEL_DISTDIR         Same as --distdir
  KYTHE_SCOPE           Same as --scope

Notes:
  - This intentionally runs nested Bazel, same as the workflow action.
  - For Bazel <8 with bzlmod enabled, this may append to MODULE.bazel.
EOF
}

DRY_RUN=0
REPO_ROOT=""
OUT_PATH="${OUT_PATH:-}"
SKIP_CXX="${SKIP_CXX:-0}"
KYTHE_DOWNLOAD_URL="${KYTHE_DOWNLOAD_URL:-$DEFAULT_KYTHE_DOWNLOAD_URL}"
BAZEL_VERSION="${USE_BAZEL_VERSION:-}"
DISTDIR="${BAZEL_DISTDIR:-}"
SCOPE="${KYTHE_SCOPE:-default}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --dry_run)
      DRY_RUN=1
      shift
      ;;
    --bazel_version)
      BAZEL_VERSION="$2"
      shift 2
      ;;
    --skip_cxx)
      SKIP_CXX=1
      shift
      ;;
    --distdir)
      DISTDIR="$2"
      shift 2
      ;;
    --scope)
      SCOPE="$2"
      shift 2
      ;;
    --repo_root)
      REPO_ROOT="$2"
      shift 2
      ;;
    --out_path)
      OUT_PATH="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Unknown flag: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ -z "$REPO_ROOT" ]]; then
        REPO_ROOT="$1"
        shift
      else
        echo "Unexpected argument: $1" >&2
        usage >&2
        exit 2
      fi
      ;;
  esac
done

if [[ $# -gt 0 ]]; then
  if [[ -z "$REPO_ROOT" ]]; then
    REPO_ROOT="$1"
    shift
  fi
fi
if [[ $# -gt 0 ]]; then
  echo "Unexpected trailing args: $*" >&2
  usage >&2
  exit 2
fi

REPO_ROOT="${REPO_ROOT:-${BUILD_WORKSPACE_DIRECTORY:-$(pwd)}}"
if [[ ! -d "$REPO_ROOT" ]]; then
  echo "Repo root does not exist: $REPO_ROOT" >&2
  exit 2
fi
REPO_ROOT="$(cd "$REPO_ROOT" && pwd)"
OUT_PATH="${OUT_PATH:-$REPO_ROOT/kythe_serving.sst}"

if [[ "$SCOPE" != "proto" && "$SCOPE" != "default" && "$SCOPE" != "full" ]]; then
  echo "Invalid --scope: $SCOPE (expected: proto|default|full)" >&2
  exit 2
fi

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

run_pipe() {
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[dry-run] $1"
  else
    eval "$1"
  fi
}

bazel_cmd_prefix() {
  if [[ -n "$BAZEL_VERSION" ]]; then
    printf "USE_BAZEL_VERSION=%q " "$BAZEL_VERSION"
  fi
}


echo "repo_root=$REPO_ROOT"
echo "out_path=$OUT_PATH"
echo "skip_cxx=$SKIP_CXX"
echo "dry_run=$DRY_RUN"
echo "kythe_download_url=$KYTHE_DOWNLOAD_URL"
echo "bazel_version=${BAZEL_VERSION:-<default>}"
echo "distdir=${DISTDIR:-<none>}"
echo "scope=$SCOPE"

if [[ ! -f "$REPO_ROOT/WORKSPACE" && ! -f "$REPO_ROOT/WORKSPACE.bazel" && ! -f "$REPO_ROOT/MODULE.bazel" ]]; then
  echo "No WORKSPACE, WORKSPACE.bazel, or MODULE.bazel found at $REPO_ROOT; skipping."
  exit 0
fi

kythe_dir_name="$(basename "${KYTHE_DOWNLOAD_URL%.tar.gz}")"
export KYTHE_DIR="$REPO_ROOT/$kythe_dir_name"

run mkdir -p "$KYTHE_DIR"
if [[ "$DRY_RUN" == "1" || ! -f "$KYTHE_DIR/extractors.bazelrc" ]]; then
  echo "Downloading Kythe from: $KYTHE_DOWNLOAD_URL"
  run_pipe "curl -sL '$KYTHE_DOWNLOAD_URL' | tar -xz -C '$KYTHE_DIR' --strip-components 1"
fi

if [[ "$DRY_RUN" == "1" ]]; then
  echo "[dry-run] patch $KYTHE_DIR/BUILD with proto_lang_toolchain load if missing"
  echo "[dry-run] append rules_proto dep to $KYTHE_DIR/MODULE.bazel if missing"
else
  # Bazel 8+ removed proto_lang_toolchain from native rules.
  if ! grep -q 'proto_lang_toolchain.bzl' "$KYTHE_DIR/BUILD" 2>/dev/null; then
    sed -i '1s|^|load("@rules_proto//proto:proto_lang_toolchain.bzl", "proto_lang_toolchain")\n|' "$KYTHE_DIR/BUILD"
  fi

  # Ensure rules_proto dep exists in Kythe module.
  if ! grep -q 'rules_proto' "$KYTHE_DIR/MODULE.bazel" 2>/dev/null; then
    echo -e '\nbazel_dep(name = "rules_proto", version = "7.1.0")' >> "$KYTHE_DIR/MODULE.bazel"
  fi
fi

pushd "$REPO_ROOT" >/dev/null

if [[ "$DRY_RUN" == "1" ]]; then
  bzl_release="$(eval "$(bazel_cmd_prefix)bazel info release" | cut -d' ' -f2 | xargs || true)"
  bzl_release="${bzl_release:-unknown}"
else
  bzl_release="$(eval "$(bazel_cmd_prefix)bazel info release" | cut -d' ' -f2 | xargs)"
fi
BZL_MAJOR_VERSION="$(echo "$bzl_release" | cut -d'.' -f1 | tr -cd '0-9')"
if [[ -z "$BZL_MAJOR_VERSION" ]]; then
  BZL_MAJOR_VERSION=9
fi

if [[ "$BZL_MAJOR_VERSION" -lt 7 ]]; then
  BZLMOD_DEFAULT=0
else
  BZLMOD_DEFAULT=1
fi

if [[ "$DRY_RUN" == "1" ]]; then
  BZLMOD_ENABLED=$BZLMOD_DEFAULT
else
  if ! eval "$(bazel_cmd_prefix)bazel info starlark-semantics" | grep -q "enable_bzlmod" ; then
    BZLMOD_ENABLED=$BZLMOD_DEFAULT
  else
    BZLMOD_ENABLED=$((1 - BZLMOD_DEFAULT))
  fi
fi

KYTHE_ARGS=""
if [[ "$BZLMOD_ENABLED" -eq 1 ]]; then
  if [[ "$BZL_MAJOR_VERSION" -lt 8 ]]; then
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "[dry-run] may append kythe dep/local_path_override to MODULE.bazel"
    else
      if ! grep -q 'bazel_dep(name = "kythe"' MODULE.bazel 2>/dev/null; then
        echo "Adding kythe repository to MODULE.bazel"
        echo -e '\nbazel_dep(name = "kythe", version = "0.0.76")' >> MODULE.bazel
        echo 'local_path_override(module_name="kythe", path="'"$KYTHE_DIR"'")' >> MODULE.bazel
      fi
    fi
  else
    KYTHE_ARGS="--inject_repository=kythe_release=$KYTHE_DIR"
  fi
else
  KYTHE_ARGS="--override_repository=kythe_release=$KYTHE_DIR"
fi

KYTHE_ARGS="$KYTHE_ARGS --experimental_extra_action_top_level_only=false --experimental_extra_action_filter=^//"
# extractors.bazelrc sets --keep_going by default; override to fail fast for local debugging.
KYTHE_ARGS="$KYTHE_ARGS --nokeep_going"

# If KYTHE_DIR lives under workspace root, avoid treating it as a local // package.
if [[ "$KYTHE_DIR" == "$REPO_ROOT"/* ]]; then
  kythe_local_pkg="${KYTHE_DIR#"$REPO_ROOT"/}"
  KYTHE_ARGS="$KYTHE_ARGS --deleted_packages=$kythe_local_pkg"
fi

if [[ "$BZL_MAJOR_VERSION" -ge 9 ]]; then
  KYTHE_ARGS="$KYTHE_ARGS --incompatible_config_setting_private_default_visibility=false"
  # Keep shared cc autoload workarounds and add java_* rules Kythe needs.
  KYTHE_ARGS="$KYTHE_ARGS --incompatible_autoload_externally=+cc_common,+CcToolchainConfigInfo,+cc_toolchain,+java_binary,+java_import,+java_library"
fi

if [[ "$SCOPE" == "proto" ]]; then
  BAZEL_TARGETS=(
    //proto/...
  )
elif [[ "$SCOPE" == "default" ]]; then
  BAZEL_TARGETS=(
    //app/...
    //server/...
    //enterprise/server/...
    //proto/...
    -//server/util/bazel/...
    -//tools/probers/...
    -//server/testutil/...
  )
else
  BAZEL_TARGETS=(//...)
fi

echo "Bazel release: $bzl_release (major=$BZL_MAJOR_VERSION, bzlmod=$BZLMOD_ENABLED)"
echo "Running Kythe-enabled build..."
echo "Kythe build scope: $SCOPE"
DISTDIR_FLAG=""
if [[ -n "$DISTDIR" ]]; then
  DISTDIR_FLAG="--distdir='$DISTDIR'"
fi
run_pipe "$(bazel_cmd_prefix)bazel --bazelrc='$KYTHE_DIR/extractors.bazelrc' build $DISTDIR_FLAG $KYTHE_ARGS -- ${BAZEL_TARGETS[*]}"

echo "Indexing kzips..."
if [[ "$DRY_RUN" == "1" ]]; then
  echo "[dry-run] ulimit -n 10240"
else
  ulimit -n 10240
fi
run rm -f kythe_entries

run_pipe "find -L bazel-out/ -name '*.go.kzip' | xargs -r -n 1 '$KYTHE_DIR/indexers/go_indexer' -continue | '$KYTHE_DIR/tools/dedup_stream' >> kythe_entries"
run_pipe "find -L bazel-out/ -name '*.proto.kzip' | xargs -r -I {} '$KYTHE_DIR/indexers/proto_indexer' -index_file {} | '$KYTHE_DIR/tools/dedup_stream' >> kythe_entries"
run_pipe "find -L bazel-out/ -name '*.java.kzip' | xargs -r -n 1 java -jar '$KYTHE_DIR/indexers/java_indexer.jar' | '$KYTHE_DIR/tools/dedup_stream' >> kythe_entries"

if [[ "$SKIP_CXX" != "1" ]]; then
  cxx_kzips="$(find -L bazel-out/*/extra_actions -name "*.cxx.kzip" 2>/dev/null || true)"
  if [[ -n "$cxx_kzips" || "$DRY_RUN" == "1" ]]; then
    echo "Running C++ indexer (installing memcached if needed)..."
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "[dry-run] ensure memcached installed (sudo apt update && sudo apt install -y memcached if missing)"
      echo "[dry-run] start memcached and index cxx kzips"
    else
      if ! command -v memcached >/dev/null 2>&1; then
        sudo apt update && sudo apt install -y memcached
      fi
      memcached -p 11211 --listen localhost -m 512 &
      memcached_pid=$!
      trap 'kill $memcached_pid >/dev/null 2>&1 || true' EXIT

      echo "$cxx_kzips" | xargs -n 1 "$KYTHE_DIR/indexers/cxx_indexer" \
        --experimental_alias_template_instantiations \
        --experimental_dynamic_claim_cache="--SERVER=localhost:11211" \
        -cache="--SERVER=localhost:11211" \
        -cache_stats \
        | "$KYTHE_DIR/tools/dedup_stream" >> kythe_entries

      kill "$memcached_pid" >/dev/null 2>&1 || true
      trap - EXIT
    fi
  fi
else
  echo "SKIP_CXX=1; skipping C++ indexer."
fi

run rm -rf kythe_tables
run "$KYTHE_DIR/tools/write_tables" --entries kythe_entries --out leveldb:kythe_tables
run mkdir -p "$(dirname "$OUT_PATH")"
run "$KYTHE_DIR/tools/export_sstable" --input leveldb:kythe_tables --output="$OUT_PATH"

echo "Wrote: $OUT_PATH"
popd >/dev/null
