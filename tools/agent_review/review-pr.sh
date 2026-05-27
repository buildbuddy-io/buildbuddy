#!/usr/bin/env bash
# review-pr.sh — Run `claude /review`, then post findings as GitHub PR review comments.
#
# Usage:
#   ./scripts/review-pr.sh              # review current branch's open PR
#   ./scripts/review-pr.sh --dry-run    # print payload without posting
#
# Requirements: claude, gh, git, jq
set -euo pipefail

DRY_RUN=false
for arg in "$@"; do
  [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

# ── Install missing tools ──────────────────────────────────────────────────────
_pkg_updated=false
apt_install() {
  if [[ "$_pkg_updated" == false ]]; then
    sudo apt-get update -q
    _pkg_updated=true
  fi
  sudo apt-get install -y "$@"
}

install_tool() {
  local cmd="$1"
  echo "==> '$cmd' not found — installing..." >&2
  case "$cmd" in
    git) apt_install git ;;
    jq)  apt_install jq  ;;
    gh)
      # gh is not in default apt repos; use the official apt source
      apt_install curl gpg
      curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg 2>/dev/null
      sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
      echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        | sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null
      sudo apt-get update -q
      sudo apt-get install -y gh
      ;;
    claude)
      curl -fsSL https://claude.ai/install.sh | bash
      export PATH="$HOME/.local/bin:$PATH"
      ;;
  esac
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: installation of '$cmd' failed." >&2
    exit 1
  fi
  echo "    '$cmd' installed." >&2
}

for cmd in git jq gh claude; do
  command -v "$cmd" &>/dev/null || install_tool "$cmd"
done

[[ -n "${REPO_TOKEN:-}" ]] && export GH_TOKEN="$REPO_TOKEN"

# ── PR metadata ────────────────────────────────────────────────────────────────
echo "==> Fetching PR info..." >&2
PR_JSON=$(gh pr view --json number,headRefOid,baseRefName 2>/dev/null) || {
  echo "Error: no open PR found for the current branch." >&2
  exit 1
}
PR_NUMBER=$(jq -r .number       <<< "$PR_JSON")
HEAD_SHA=$(jq  -r .headRefOid   <<< "$PR_JSON")
BASE_REF=$(jq  -r .baseRefName  <<< "$PR_JSON")
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
echo "    PR #$PR_NUMBER on $REPO  (head: ${HEAD_SHA:0:8}, base: $BASE_REF)" >&2

# ── Skip if already reviewed ───────────────────────────────────────────────────
# Prevents re-running on subsequent pushes to the same PR.
# Set FORCE=1 to override and post a new review anyway.
if [[ -z "${FORCE:-}" ]]; then
  echo "==> Checking for existing reviews..." >&2
  EXISTING_BOT_REVIEWS=$(gh api "repos/$REPO/pulls/$PR_NUMBER/reviews" \
    --jq '[.[] | select(.user.type == "Bot")] | length')
  if [[ "$EXISTING_BOT_REVIEWS" -gt 0 ]]; then
    echo "==> PR #$PR_NUMBER already has a bot review — skipping (set FORCE=1 to override)." >&2
    exit 0
  fi
fi

# ── Run the review ─────────────────────────────────────────────────────────────
# Note: do NOT pass --bare here; skills/plugins need to be active for /review.
# Permissions are written to a temp config dir to avoid CLI flag parsing issues
# with space-containing Bash patterns.
CLAUDE_SETTINGS=$(mktemp -d)
trap 'rm -rf "$CLAUDE_SETTINGS"' EXIT
cat > "$CLAUDE_SETTINGS/settings.json" <<'EOF'
{
  "permissions": {
    "allow": [
      "Read",
      "Bash(git diff *)",
      "Bash(git fetch *)",
      "Bash(git log *)",
      "Bash(git show *)",
      "Bash(git blame *)",
      "Bash(gh pr diff *)",
      "Bash(gh pr view *)"
    ]
  }
}
EOF

echo "==> Running claude /review  (this may take a minute)..." >&2
REVIEW_TEXT=$(CLAUDE_CONFIG_DIR="$CLAUDE_SETTINGS" claude --print \
  "Review PR #$PR_NUMBER. /review

For every finding, include the exact file path and line number in the format \`path/to/file.go:42\` so findings can be posted as inline GitHub comments.") || {
  echo "Error: claude /review failed." >&2
  exit 1
}

if [[ ! "$REVIEW_TEXT" =~ [^[:space:]] ]]; then
  echo "Error: review produced no output." >&2
  exit 1
fi

# ── Convert review output to structured JSON ───────────────────────────────────
echo "==> Structuring review output into JSON..." >&2

read -r -d '' PARSE_PROMPT <<'PROMPT' || true
Parse the code review below and output ONLY a valid JSON object — no markdown fences, no explanation.

Schema:
{
  "summary": "<concise overall summary as markdown, max ~3 sentences>",
  "comments": [
    {
      "file": "<file path relative to repo root, e.g. server/foo.go>",
      "line": <integer — the line number in the NEW version of the file>,
      "body": "<full comment text as markdown>",
      "severity": "<critical | warning | suggestion>"
    }
  ]
}

Rules:
- Include a comment entry for every finding that mentions a file path, even if the line number is approximate — use the nearest relevant line you can infer from context.
- Findings with no file reference at all go into "summary".
- File paths must be relative (no leading slash, no absolute paths).
- Output ONLY the JSON object. No other text before or after it.

REVIEW:
PROMPT

REVIEW_JSON=$(printf '%s\n\n%s' "$PARSE_PROMPT" "$REVIEW_TEXT" | claude --print --allowedTools "") || {
  echo "Warning: failed to parse review into JSON — posting as a single body comment." >&2
  gh api "repos/$REPO/pulls/$PR_NUMBER/reviews" \
    --method POST \
    -F commit_id="$HEAD_SHA" \
    -f event="COMMENT" \
    -f body="$REVIEW_TEXT"
  echo "Done: posted review as a single comment." >&2
  exit 0
}

# Strip accidental markdown fences that some models add despite instructions
REVIEW_JSON=$(sed '/^```/d' <<< "$REVIEW_JSON")

if ! jq -e . <<< "$REVIEW_JSON" >/dev/null 2>&1; then
  echo "Warning: structured JSON is invalid — posting as a single body comment." >&2
  gh api "repos/$REPO/pulls/$PR_NUMBER/reviews" \
    --method POST \
    -F commit_id="$HEAD_SHA" \
    -f event="COMMENT" \
    -f body="$REVIEW_TEXT"
  echo "Done: posted review as a single comment." >&2
  exit 0
fi

SUMMARY=$(jq -r .summary <<< "$REVIEW_JSON")
TOTAL_COMMENTS=$(jq '.comments | length' <<< "$REVIEW_JSON")
echo "    Found $TOTAL_COMMENTS candidate line-level comment(s)." >&2

# ── Build set of lines that are actually in the diff ──────────────────────────
# GitHub only accepts inline comments on lines present in the diff hunks.
# We fetch the raw diff and track every new-file line number that appears on
# the RIGHT side (added + context lines), per file.

echo "==> Fetching PR diff to validate line numbers..." >&2
DIFF=$(gh api "repos/$REPO/pulls/$PR_NUMBER" \
  -H "Accept: application/vnd.github.diff") 2>/dev/null \
  || DIFF=$(git diff "$(git merge-base HEAD "origin/$BASE_REF")"..HEAD)

# Parse diff → write "file<TAB>line" for every RIGHT-side line to a temp file
VALID_LINES=$(mktemp)
trap 'rm -f "$VALID_LINES"' EXIT

current_file=""
new_line=0
while IFS= read -r diff_line; do
  # +++ b/path/to/file  →  start of a new file section
  if [[ "$diff_line" =~ ^\+\+\+\ b/(.+)$ ]]; then
    current_file="${BASH_REMATCH[1]}"
    new_line=0
  # @@ -old_start[,count] +new_start[,count] @@  →  new hunk
  elif [[ "$diff_line" =~ ^@@\ -[0-9]+(,[0-9]+)?\ \+([0-9]+)(,[0-9]+)?\ @@ ]]; then
    new_line=$(( BASH_REMATCH[2] - 1 ))
  elif [[ -n "$current_file" ]]; then
    case "$diff_line" in
      -*)
        # Deleted line: exists only on LEFT side, don't advance new_line
        ;;
      +*|" "*)
        # Added or context line: exists on RIGHT side
        (( new_line++ )) || true
        printf '%s\t%d\n' "$current_file" "$new_line" >> "$VALID_LINES"
        ;;
    esac
  fi
done <<< "$DIFF"

# ── Partition comments: inline vs. body ───────────────────────────────────────
INLINE_COMMENTS="[]"
BODY_OVERFLOW=""

while IFS= read -r comment_json; do
  file=$(jq -r .file     <<< "$comment_json")
  line=$(jq -r .line     <<< "$comment_json")
  body=$(jq -r .body     <<< "$comment_json")
  sev=$(jq  -r .severity <<< "$comment_json")

  if grep -qFx "$(printf '%s\t%s' "$file" "$line")" "$VALID_LINES" 2>/dev/null; then
    INLINE_COMMENTS=$(jq \
      --arg  path "$file" \
      --argjson line "$line" \
      --arg  body "**[$sev]** $body" \
      '. += [{"path": $path, "line": $line, "side": "RIGHT", "body": $body}]' \
      <<< "$INLINE_COMMENTS")
  else
    # Line not in this diff — include in the review body instead
    BODY_OVERFLOW+=$'\n'"- **[$sev]** \`$file:$line\` — $body"
  fi
done < <(jq -c '.comments[]?' <<< "$REVIEW_JSON")

FULL_BODY="$SUMMARY"
if [[ -n "$BODY_OVERFLOW" ]]; then
  FULL_BODY+=$'\n\n'"### Additional findings (lines outside the diff)$BODY_OVERFLOW"
fi

INLINE_COUNT=$(jq length <<< "$INLINE_COMMENTS")
OVERFLOW_COUNT=$(( TOTAL_COMMENTS - INLINE_COUNT ))
echo "    $INLINE_COUNT inline comment(s), $OVERFLOW_COUNT folded into body." >&2

# Full body used as fallback if the inline POST fails — includes all inline
# findings serialized as a markdown list so nothing is silently dropped.
FULL_BODY_FALLBACK="$FULL_BODY"
if [[ "$INLINE_COUNT" -gt 0 ]]; then
  INLINES_AS_TEXT=$(jq -r '.[] | "- `\(.path):\(.line)` — \(.body)"' <<< "$INLINE_COMMENTS")
  FULL_BODY_FALLBACK+=$'\n\n'"### Inline findings$'\n'$INLINES_AS_TEXT"
fi

# ── Build and post the review payload ─────────────────────────────────────────
PAYLOAD=$(jq -n \
  --arg  commit_id "$HEAD_SHA" \
  --arg  body      "$FULL_BODY" \
  --argjson comments "$INLINE_COMMENTS" \
  '{commit_id: $commit_id, event: "COMMENT", body: $body, comments: $comments}')

if [[ "$DRY_RUN" == "true" ]]; then
  echo "==> Dry run — payload that would be posted:"
  jq . <<< "$PAYLOAD"
  exit 0
fi

echo "==> Payload to be posted:" >&2
jq . <<< "$PAYLOAD" >&2

echo "==> Posting review to GitHub..." >&2
RESPONSE=$(gh api "repos/$REPO/pulls/$PR_NUMBER/reviews" \
  --method POST \
  --input - <<< "$PAYLOAD") || {
  echo "Warning: POST with inline comments failed (likely invalid line numbers) — retrying as single body comment." >&2
  RESPONSE=$(gh api "repos/$REPO/pulls/$PR_NUMBER/reviews" \
    --method POST \
    -F commit_id="$HEAD_SHA" \
    -f event="COMMENT" \
    -f body="$FULL_BODY_FALLBACK") || {
    echo "Error: gh api POST failed (exit $?). Check that REPO_TOKEN has pull_requests: write permission." >&2
    exit 1
  }
}

echo "==> GitHub response:" >&2
jq . <<< "$RESPONSE" >&2

REVIEW_URL=$(jq -r '.html_url // empty' <<< "$RESPONSE")
if [[ -z "$REVIEW_URL" ]]; then
  echo "Error: review posted but no html_url in response." >&2
  exit 1
fi
echo "Done: $REVIEW_URL" >&2
