#!/usr/bin/env bash

set -euo pipefail

do_not_recycle_marker_dir() {
  if [[ -n "${BUILDBUDDY_CI_RUNNER_ABSPATH:-}" ]]; then
    dirname "${BUILDBUDDY_CI_RUNNER_ABSPATH}"
    return
  fi
  pwd
}

mark_do_not_recycle() {
  local marker_dir
  marker_dir="$(do_not_recycle_marker_dir)"
  touch "${marker_dir}/.BUILDBUDDY_DO_NOT_RECYCLE"
}

if [[ -z "${SLACK_CHANGELOG_WEBHOOK_URL:-}" ]]; then
  echo "SLACK_CHANGELOG_WEBHOOK_URL is not set; skipping."
  mark_do_not_recycle
  exit 0
fi

commit="${GIT_COMMIT:-$(git rev-parse HEAD)}"
before_sha="$(git rev-parse "${commit}^1" 2>/dev/null || true)"
if [[ -z "${before_sha}" ]]; then
  before_sha="$(git rev-list --max-parents=0 "${commit}")"
fi

added="$(
  git diff --name-status "${before_sha}" "${commit}" -- "website/changelog/*.md" "website/changelog/*.mdx" \
    | awk '$1 == "A" { print $2 }'
)"

if [[ -z "${added}" ]]; then
  echo "No newly added changelog entries."
  mark_do_not_recycle
  exit 0
fi

msg=$'*New changelog entries added:*\n'
while IFS= read -r file; do
  [[ -z "${file}" ]] && continue
  slug="$(basename "${file}")"
  slug="${slug%.*}"
  msg+=$'• https://www.buildbuddy.io/changelog/'"${slug}"$'\n'
done <<< "${added}"

payload="$(
  MESSAGE="${msg}" python3 - <<'PY'
import json
import os

print(json.dumps({"attachments": [{"color": "#1f883d", "text": os.environ["MESSAGE"]}]}))
PY
)"

curl -fsSL -X POST -H "Content-Type: application/json" --data "${payload}" "${SLACK_CHANGELOG_WEBHOOK_URL}"
