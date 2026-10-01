#!/usr/bin/env bash
# Builds the container images defined under dockerfiles/ to check that they
# still build. Nothing is pushed.
#
# Usage: dockerfiles/build_images.sh [DIR...]
#
# DIRs are relative to dockerfiles/ (e.g. rbe-ubuntu22-04). By default, every
# directory containing a Dockerfile is built.
set -euo pipefail

cd "$(dirname "$0")"

PLATFORM="${PLATFORM:-linux/amd64}"

if (($# > 0)); then
  dirs=("$@")
else
  mapfile -t dirs < <(find . -name Dockerfile -printf '%h\n' | sed 's|^\./||' | sort)
fi
if ((${#dirs[@]} == 0)); then
  echo "No Dockerfiles found" >&2
  exit 1
fi

docker version --format 'Docker {{.Server.Version}}'

failed=()
summary=()
for dir in "${dirs[@]}"; do
  tag="build-images-check/${dir//\//-}:latest"
  echo "::: Building $dir for $PLATFORM"
  start=$SECONDS
  # --pull and --no-cache so that each run picks up current base images and
  # package versions, as a release build would.
  if docker buildx build \
    --platform="$PLATFORM" \
    --pull \
    --no-cache \
    --progress=plain \
    --tag="$tag" \
    "$dir"; then
    size=$(docker image inspect --format '{{.Size}}' "$tag" | numfmt --to=iec)
    summary+=("PASS  $dir  ($((SECONDS - start))s, $size)")
  else
    failed+=("$dir")
    summary+=("FAIL  $dir  ($((SECONDS - start))s)")
  fi
  # Free disk space before the next build.
  docker image rm --force "$tag" >/dev/null 2>&1 || true
  docker builder prune --all --force >/dev/null
done

echo
echo "::: Summary ($PLATFORM)"
printf '  %s\n' "${summary[@]}"
if ((${#failed[@]} > 0)); then
  echo "${#failed[@]} of ${#dirs[@]} images failed to build" >&2
  exit 1
fi
