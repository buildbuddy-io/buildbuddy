#!/usr/bin/env bash
# Measures cold-build cost for one or more bazel targets.
#
# For each target it reports:
#   - number of external deps fetched
#   - total bytes downloaded for external deps
#   - cold build wall time
#   - cold build size on disk (output_base + repository_cache)
#
# Each target is built in its own ephemeral --output_base and --repository_cache
# with remote cache, remote executor, and remote downloader explicitly disabled,
# so the numbers reflect a from-scratch local build with no BuildBuddy assist.
#
# Usage: tools/cost/cold_build.sh [--keep] //target/one //target/two ...
#        tools/cost/cold_build.sh --json //... > report.json

set -euo pipefail

KEEP=0
JSON=0
TARGETS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1 ;;
    --json) JSON=1 ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) TARGETS+=("$arg") ;;
  esac
done

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "usage: $0 [--keep] [--json] <bazel-target>..." >&2
  exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

workdir="$(mktemp -d -t bb-cold-build.XXXXXX)"
cleanup() {
  [[ $KEEP -eq 1 ]] && return
  chmod -R u+w "$workdir" 2>/dev/null || true
  rm -rf "$workdir"
}
trap cleanup EXIT

# Build flags: belt-and-suspenders disabling of any remote/disk caching that
# might have been turned on by shared.bazelrc or default registry mirrors.
# (We deliberately ignore .bazelrc/user.bazelrc via --noworkspace_rc below.)
build_flags=(
  --remote_cache=
  --remote_executor=
  --experimental_remote_downloader=
  --bes_backend=
  --bes_results_url=
  --disk_cache=
  --noremote_accept_cached
  --noremote_upload_local_results
)

results=()

for target in "${TARGETS[@]}"; do
  safe="$(echo "$target" | tr '/:@' '___')"
  base="$workdir/$safe"
  output_base="$base/ob"
  repo_cache="$base/rc"
  profile="$base/profile.json.gz"
  exec_log="$base/exec.log"
  mkdir -p "$output_base" "$repo_cache"

  echo "== cold build: $target ==" >&2
  echo "   output_base=$output_base" >&2
  echo "   repo_cache=$repo_cache" >&2

  # Use a custom startup --output_base. We DO want shared.bazelrc to apply
  # (so toolchains resolve as in real builds), but we want to neutralize
  # user.bazelrc's remote settings. Easiest: pass --bazelrc=shared.bazelrc
  # explicitly and skip user.bazelrc.
  start=$(date +%s.%N)
  bazel \
    --noworkspace_rc \
    --nohome_rc \
    --nosystem_rc \
    --bazelrc="$repo_root/shared.bazelrc" \
    --output_base="$output_base" \
    build \
    --repository_cache="$repo_cache" \
    --profile="$profile" \
    "${build_flags[@]}" \
    "$target" >"$base/build.log" 2>&1 || {
      echo "BUILD FAILED for $target; see $base/build.log" >&2
      tail -40 "$base/build.log" >&2 || true
      continue
    }
  end=$(date +%s.%N)
  wall=$(awk -v s="$start" -v e="$end" 'BEGIN{printf "%.2f", e-s}')

  # External deps: count fetched repos in the output_base.
  # rules_go and bzlmod put fetched repos in $output_base/external/.
  # Empty marker dirs are also created; only count ones with content.
  ext_dir="$output_base/external"
  ext_count=0
  if [[ -d "$ext_dir" ]]; then
    ext_count=$(find "$ext_dir" -mindepth 1 -maxdepth 1 \( -type d -o -type l \) \
                  ! -name '*.marker' ! -name '@*' 2>/dev/null | wc -l | tr -d ' ')
  fi

  # Bytes downloaded: size of repository_cache content/ + downloads/.
  # bazel writes every fetched archive there before extracting.
  rc_bytes=0
  if [[ -d "$repo_cache" ]]; then
    rc_bytes=$(du -sb "$repo_cache" 2>/dev/null | awk '{print $1}')
  fi

  # Cold-build size on disk = output_base + repository_cache.
  ob_bytes=$(du -sb "$output_base" 2>/dev/null | awk '{print $1}')
  total_bytes=$((ob_bytes + rc_bytes))

  results+=("$target|$ext_count|$rc_bytes|$wall|$total_bytes|$ob_bytes")
done

human() {
  local b=$1
  awk -v b="$b" 'BEGIN{
    split("B KB MB GB TB", u);
    i=1; while (b>=1024 && i<5) { b/=1024; i++ }
    printf (i==1 ? "%d %s" : "%.1f %s"), b, u[i]
  }'
}

if [[ $JSON -eq 1 ]]; then
  printf '['
  first=1
  for r in "${results[@]}"; do
    IFS='|' read -r t ec rb w tb ob <<<"$r"
    [[ $first -eq 0 ]] && printf ','
    first=0
    printf '\n  {"target":"%s","external_deps":%d,"bytes_downloaded":%d,"cold_build_seconds":%s,"size_on_disk_bytes":%d,"output_base_bytes":%d}' \
      "$t" "$ec" "$rb" "$w" "$tb" "$ob"
  done
  printf '\n]\n'
else
  printf '\n%-50s %8s %12s %10s %12s\n' \
    "TARGET" "#DEPS" "DOWNLOAD" "WALL(s)" "DISK"
  printf '%s\n' "$(printf '%.0s-' {1..96})"
  for r in "${results[@]}"; do
    IFS='|' read -r t ec rb w tb ob <<<"$r"
    printf '%-50s %8d %12s %10s %12s\n' \
      "$t" "$ec" "$(human "$rb")" "$w" "$(human "$tb")"
  done
fi

if [[ $KEEP -eq 1 ]]; then
  echo "" >&2
  echo "workdir kept at: $workdir" >&2
fi
