package review

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/agent/agentflags"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/cli/util/agent"
	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil"
	"github.com/google/go-github/v59/github"
)

const Usage = `
usage: bb agent review [ --dry_run ] [ --force ] [ --allow_fork ]

Reviews the open GitHub pull request for the checked-out branch, then posts the
findings as PR review comments.

The review reads the source from the local checkout, so to review another PR,
check it out first with ` + "`gh pr checkout <pr>`" + `.

Requires gh and a GitHub token in REPO_TOKEN, GH_TOKEN, or GITHUB_TOKEN.

Drafts and PRs that already have a bot review are skipped; pass --force (or
set AGENT_REVIEW_FORCE=1) to review them anyway.

By default, PRs from forks are refused, because reviewing one runs the agent over
untrusted code; pass --allow_fork to review one anyway.

Examples:
  bb agent review
  bb agent review --dry_run
`

// Flags holds the flags unique to this subcommand. The flags shared by all
// `bb agent` subcommands are registered on it by the agent package.
var (
	Flags = flag.NewFlagSet("review", flag.ContinueOnError)

	dryRun    = Flags.Bool("dry_run", false, "Print the review payload without posting it.")
	force     = Flags.Bool("force", false, "Review the PR even if it is a draft or already has a bot review.")
	allowFork = Flags.Bool("allow_fork", false, "Review the PR even if it comes from a fork. See the warning that --allow_fork suppresses.")
)

// reviewAllowedTools restricts the review agent to reading the repo and the PR.
var reviewAllowedTools = []string{
	"Read",
	"Glob",
	"Grep",
	"Bash(git diff *)",
	"Bash(git fetch *)",
	"Bash(git log *)",
	"Bash(git show *)",
	"Bash(git blame *)",
	"Bash(gh pr diff *)",
	"Bash(gh pr view *)",
}

// editPrompt asks the agent to edit the review for its reader and convert it
// to JSON. The review agent focuses on finding problems; this pass focuses on
// how they read.
const editPrompt = `You are editing a code review before it is posted on a pull request, and
converting it to JSON. The reviewer has done the analysis. Your job is to make
the review concise, clear and pleasant to receive. Don't change what any
finding claims, and don't add facts. You may cut detail, merge duplicates,
split a finding that covers unrelated issues, and reword anything.

The reader may be the PR's author or a reviewer. Assume they know the system
at a high level, but not this change's code: an agent may have written it,
so even the author may not remember its function, type or variable names,
and a reviewer may know little about the change beyond its description. In
the pitch, describe behavior, not code: what the system does, and what will
go wrong. Names that are part of the design, such as flags, config keys or
API fields, are fine. Code names belong in Details, where they help someone
find the code.

The reader should read each comment once and know what's wrong and what to
do. If they would want to ask for a summary, the comment is too long.

For each finding:
- Open with a pitch of two or three short sentences: what goes wrong, why,
  and what should be done. Lead with the consequence. Use plain words, one
  idea per sentence, and no asides in parentheses. For example:

    This can show one user's data to another. The response cache no longer
    includes the user ID in its key, so two users requesting the same path
    share a cache entry. The user ID should be added back to the key.

- Most findings need nothing more. If a reader who wants to dig in needs
  more, put it after the pitch in a section starting with "**Details:**".
  Details answer "how do I know this is true, and what are my options?": the
  mechanism, at most one example, and the main alternative fix if there is
  one. They are not a complete record of what the reviewer found. Order them
  so the reader can stop at any point, most important first.
- Cut proof the reader can get by reading the code, repeated or variant
  scenarios, hedging that argues both sides, extra fixes when one is clearly
  best, and closing restatements.
- Keep the reviewer's position, and keep uncertainty ("I couldn't check X"),
  stated briefly.

Write like a helpful colleague: direct but not curt, with no praise or
filler.

Output ONLY a valid JSON object, with no markdown fences and no explanation:
{
  "summary": "<what the PR does, in one or two sentences of markdown>",
  "comments": [
    {
      "file": "<file path relative to repo root, e.g. server/foo.go>",
      "line": <integer, the line number in the NEW version of the file>,
      "category": "<before_merge, follow_up or discussion, from the finding's label: Address before merge, Address in a follow-on or Item for discussion>",
      "body": "<the edited comment as markdown, without the label>"
    }
  ]
}

Rules:
- Every finding becomes a comment entry, with the file and line the reviewer
  gave. If a finding has no location, attach it to the changed file and line
  it relates to most closely.
- File paths must be relative, with no leading slash.
- Remove footnote-style references such as "(#1)" from comment bodies and the
  summary; GitHub turns them into issue links.
- Omit comments that only praise or acknowledge the change without raising a
  concern.
- The summary only explains what the PR does. It must not contain findings,
  caveats or notes, and doesn't summarize the comments.

REVIEW:
`

// reviewPrompt is a format string taking the PR number. It is deliberately
// generic: repo-specific guidance comes from the repo's own CLAUDE.md or
// AGENTS.md.
const reviewPrompt = `Review PR #%d. Only report findings: don't build, run, or test the code, and
don't edit files, commit, push, or post to GitHub.

## Process

1. Read the PR description and the diff to understand what the change is for.
2. Read the repo's guidance files (CLAUDE.md, AGENTS.md, CONTRIBUTING and
   similar) for its conventions.
3. Read the surrounding code as needed: callers, related code and existing
   tests. Read enough to know what the change affects and how similar things
   are done elsewhere in the repo.
4. Look for broken assumptions. For each behavior the change alters, ask
   what depends on it: other code, tools, users, stored data, other services
   or versions. Include guarantees that were never written down, such as
   ordering, determinism, timing, error behavior and formats. Then check
   whether they still hold. Many regressions sit outside the diff, in code
   that was correct until something it relied on changed.
5. Collect findings, verify each one against the code, and drop any that
   don't hold up.

## Goal

Catch the problems that matter, without noise. In priority order:

1. **Prevent regressions.** The change must not break behavior that works today.
2. **Make it a good change.** It should be correct and tested, and the code
   should be no harder to maintain than before.

Simple is better than complicated. The most valuable design feedback is "if
you did X instead, all of Y would go away": a simpler approach that makes a
large part of the change, or of the existing code, unnecessary. Look for these
actively. Simple is also better than clever: a dense line that would be
clearer unpacked is worth a mention, but matters much less.

Apply the same standard to your own suggestions. Don't suggest added
abstraction, configurability or defensive code unless a concrete problem
requires it.

A review with no findings is a good outcome when the change is sound. Every
comment costs the author time, so only write ones you'd defend to the engineer
who owns this code.

## Categories

Start every finding with exactly one of these labels:

**Address before merge:** issues that will cause harm, to users or the
system, if the PR ships as is. Typically:
- bugs and regressions: wrong results, crashes, data loss, security holes
- broken compatibility with existing clients, stored data or config
- real performance regressions
- missing tests for new or changed behavior
- severe design problems that make the code much harder to change correctly

If a finding would cause harm, it belongs here even when you're unsure or
the fix needs discussion; say so in the comment. Stay within the PR's scope:
a pre-existing problem belongs here only if this change makes it reachable
or worse.

**Address in a follow-on:** worth doing, but in a separate PR. This is
usually a refactor for a design concern, lower-priority test coverage, or a
significant pre-existing problem you noticed nearby.

**Item for discussion:** use rarely, for an issue that seems worth raising
but doesn't fit either category above. For example, a substantially simpler
or better approach to the whole change.

## What counts as a finding

- Verify against the code before reporting. Trace the callers and check
  whether the case is already handled elsewhere. If you can't describe a
  concrete situation where it goes wrong, drop it. If you're unsure but the
  stakes are high, report it and say what you couldn't confirm.
- Breaking the repo's conventions can be a finding. Your own preferences
  are not.
- Skip: style, naming and formatting; anything a linter or compiler catches;
  hypothetical hardening for cases that can't happen; restating what the code
  does; praise.
- One finding per issue. If it repeats, list the other places in the same
  comment.

## Writing

Write each finding plainly, with the evidence that supports it. A separate
editing pass will tighten the wording, so focus on being accurate and
complete, and keep your position on what should be done.

Give each finding's location as ` + "`path/to/file.go:42`" + ` so it can be posted as an
inline comment. Every finding needs a location, so it can be resolved on its
own. For a finding about the PR as a whole, use the changed line it relates
to most closely.
`

// reviewLinkMarker identifies the comment posted by postReviewLink, so that
// it's only posted once per PR.
const reviewLinkMarker = "<!-- bb-agent-review-link -->"

const reviewDocsURL = "https://www.buildbuddy.io/docs/cli-commands#bb-agent-review"

var (
	hunkRe = regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@`)
	fileRe = regexp.MustCompile(`^\+\+\+ b/(.+)$`)
)

type reviewComment struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Category string `json:"category"`
	Body     string `json:"body"`
}

// categoryLabels maps a review category to the label prefixed to the comment.
var categoryLabels = map[string]string{
	"before_merge": "**Address before merge:**",
	"follow_up":    "**Address in a follow-on:**",
	"discussion":   "**Item for discussion:**",
}

// text returns the comment body, prefixed with its category label. A missing
// or unknown category yields the body without a label.
func (c reviewComment) text() string {
	if label, ok := categoryLabels[c.Category]; ok {
		return label + " " + c.Body
	}
	return c.Body
}

type reviewJSON struct {
	Summary  string          `json:"summary"`
	Comments []reviewComment `json:"comments"`
}

type fileLine struct {
	file string
	line int
}

// HandleReview receives only the positional args; the agent package parses
// Flags before calling it.
func HandleReview(args []string) (int, error) {
	if len(args) != 0 {
		log.Print(Usage)
		return 1, nil
	}
	if _, err := exec.LookPath("gh"); err != nil {
		return -1, fmt.Errorf("gh is not installed or not in PATH")
	}

	ctx := context.Background()
	gh, err := newGHClient()
	if err != nil {
		return -1, err
	}

	// Fetch the PR associated with the current, checked-out branch.
	owner, repo, branch, err := fetchRepoInfo()
	if err != nil {
		return -1, err
	}
	log.Printf("Fetching PR info...")
	prs, _, err := gh.PullRequests.List(ctx, owner, repo, &github.PullRequestListOptions{
		Head:  owner + ":" + branch,
		State: "open",
	})
	if err != nil {
		return -1, fmt.Errorf("list PRs: %w", err)
	}
	if len(prs) == 0 {
		return -1, fmt.Errorf("no open PR found for branch %q", branch)
	}
	pr := prs[0]

	headSHA := pr.GetHead().GetSHA()
	headShort := headSHA
	if len(headShort) > 8 {
		headShort = headShort[:8]
	}
	log.Printf("PR #%d on %s/%s  (head: %s, base: %s)", pr.GetNumber(), owner, repo, headShort, pr.GetBase().GetRef())

	if !*allowFork && isFork(pr, owner, repo) {
		log.Warnf("Reviewing this code runs an agent over untrusted code, which the agent reads as input. Proceed with caution. Read the diff yourself first, and pass --allow_fork to review it anyway.")
		return 1, nil
	}

	// A PR is reviewed once, when it's ready for review, so skip drafts and PRs
	// that already have a bot review to avoid spamming them with reviews.
	if !*force && os.Getenv("AGENT_REVIEW_FORCE") != "1" {
		skip, err := shouldSkip(ctx, gh, owner, repo, pr)
		if err != nil {
			return -1, err
		}
		if skip {
			return 0, nil
		}
	}

	// Have the agent generate the review.
	log.Printf("%sRunning agent to review the PR (this may take a few minutes)...%s", terminal.Esc(90), terminal.Esc())
	reviewBuf := &bytes.Buffer{}
	err = agent.Run(ctx, &agentutil.RunRequest{
		Agent:              *agentflags.Agent,
		Model:              *agentflags.Model,
		ReasoningEffort:    *agentflags.Effort,
		Prompt:             fmt.Sprintf(reviewPrompt, pr.GetNumber()),
		ClaudeAllowedTools: reviewAllowedTools,
		// The agent reads the PR with gh, which needs network access. Codex only
		// allows network access in the workspace-write sandbox, so the agent can
		// also write to the workspace; it is told not to edit anything.
		CodexSandbox: agentutil.SandboxWorkspaceWrite,
		CodexArgs:    []string{"--config", "sandbox_workspace_write.network_access=true"},
		Output:       reviewBuf,
		Progress:     io.Discard,
	})
	if err != nil {
		return -1, fmt.Errorf("error running review agent: %w", err)
	}
	reviewText := reviewBuf.String()
	if strings.TrimSpace(reviewText) == "" {
		return -1, fmt.Errorf("review produced no output.")
	}
	if *dryRun {
		fmt.Println("Dry run — review before editing:")
		fmt.Println(reviewText)
	}

	// Have the agent edit the review and convert it to JSON. If that fails,
	// post the unstructured review as a single body comment.
	log.Printf("Editing and structuring review...")
	review, err := structureReview(ctx, reviewText)
	if err != nil {
		log.Warnf("Failed to structure review (%s); posting it as a single body comment.", err)
		return postReview(ctx, gh, owner, repo, pr, reviewText, nil)
	}
	log.Printf("    Found %d candidate line-level comment(s).", len(review.Comments))

	// GitHub only accepts inline comments on lines present in the diff hunks.
	log.Printf("Fetching PR diff to validate line numbers...")
	diff, _, err := gh.PullRequests.GetRaw(ctx, owner, repo, pr.GetNumber(), github.RawOptions{Type: github.Diff})
	if err != nil {
		return -1, fmt.Errorf("fetch PR diff: %w", err)
	}
	validLines := diffLines(diff)

	// Post every comment inline, so each can be resolved on its own. Only if
	// the diff has no lines at all do comments fall back to the review body.
	var inlineComments []*github.DraftReviewComment
	var overflowLines []string
	moved := 0
	for _, c := range review.Comments {
		loc, ok := placeComment(c, validLines)
		if !ok {
			overflowLines = append(overflowLines, fmt.Sprintf("- `%s:%d` — %s", c.File, c.Line, c.text()))
			continue
		}
		body := c.text()
		if loc != (fileLine{c.File, c.Line}) {
			moved++
			body += fmt.Sprintf("\n\n<sub>About `%s:%d`, which isn't in the diff.</sub>", c.File, c.Line)
		}
		inlineComments = append(inlineComments, &github.DraftReviewComment{
			Path: new(loc.file),
			Line: new(loc.line),
			Side: new("RIGHT"),
			Body: new(body),
		})
	}

	fullBody := review.Summary
	if len(overflowLines) > 0 {
		fullBody += "\n\n### Additional findings\n" + strings.Join(overflowLines, "\n")
	}
	log.Printf("    %d inline comment(s), %d moved to the nearest diff line, %d folded into body.", len(inlineComments), moved, len(overflowLines))

	return postReview(ctx, gh, owner, repo, pr, fullBody, inlineComments)
}

// isFork reports whether the PR's head branch lives in the given repo. If it doesn't,
// it's assumed to be from a fork.
func isFork(pr *github.PullRequest, owner, repo string) bool {
	return pr.GetHead().GetRepo().GetFullName() != owner+"/"+repo
}

// shouldSkip reports whether the PR is a draft or already has a bot review.
func shouldSkip(ctx context.Context, gh *github.Client, owner, repo string, pr *github.PullRequest) (bool, error) {
	log.Printf("Checking for existing reviews...")
	// Only the first page of reviews is checked.
	reviews, _, err := gh.PullRequests.ListReviews(ctx, owner, repo, pr.GetNumber(), nil)
	if err != nil {
		return false, fmt.Errorf("fetch existing reviews: %w", err)
	}
	hasBotReview, hasReviewLink := false, false
	for _, r := range reviews {
		if hasReviewLink && hasBotReview {
			break
		}
		if strings.Contains(r.GetBody(), reviewLinkMarker) {
			// The review link posted on drafts doesn't count as a review of the code.
			hasReviewLink = true
		} else if r.GetUser().GetType() == "Bot" {
			hasBotReview = true
		}
	}

	if pr.GetDraft() {
		log.Printf("PR #%d is a draft — skipping (pass --force or set AGENT_REVIEW_FORCE=1 to override).", pr.GetNumber())
		// Only post the review link if it hasn't been posted and the PR hasn't
		// already been reviewed (e.g. by a forced run).
		if !hasReviewLink && !hasBotReview {
			if err := postReviewLink(ctx, gh, owner, repo, pr); err != nil {
				log.Warnf("Failed to post review link comment: %s", err)
			}
		}
		return true, nil
	}
	if hasBotReview {
		log.Printf("PR #%d already has a bot review — skipping (pass --force or set AGENT_REVIEW_FORCE=1 to override).", pr.GetNumber())
		return true, nil
	}
	return false, nil
}

// postReviewLink posts a comment with a link to trigger a review.
func postReviewLink(ctx context.Context, gh *github.Client, owner, repo string, pr *github.PullRequest) error {
	// Keep these in sync with the env var names in ci_runner_env.
	actionName := os.Getenv("BUILDBUDDY_ACTION_NAME")
	if actionName == "" || os.Getenv("BUILDBUDDY_TRIGGER_EVENT") != "pull_request" {
		return nil
	}

	runURL, err := runReviewURL(pr, actionName)
	if err != nil {
		return err
	}
	body := reviewLinkMarker + "\n" +
		"**Agent code review** · Get feedback on this pull request before a teammate reviews it.\n" +
		"**[Run review](" + runURL + ")**\n\n" +
		"<sub>Runs [`bb agent review`](" + reviewDocsURL + ") on BuildBuddy Workflows</sub>"

	if *dryRun {
		fmt.Println("Dry run — review link comment that would be posted:")
		fmt.Println(body)
		return nil
	}
	req := &github.PullRequestReviewRequest{
		CommitID: new(pr.GetHead().GetSHA()),
		Event:    new("COMMENT"),
		Body:     new(body),
	}
	if _, _, err := gh.PullRequests.CreateReview(ctx, owner, repo, pr.GetNumber(), req); err != nil {
		return fmt.Errorf("post review: %w", err)
	}
	log.Printf("Posted review link comment to PR #%d.", pr.GetNumber())
	return nil
}

// runReviewURL returns a link that runs the given workflow action on the PR's
// branch with AGENT_REVIEW_FORCE=1.
func runReviewURL(pr *github.PullRequest, actionName string) (string, error) {
	runURL, err := url.JoinPath(*agentflags.HTTPTarget, "workflows", "run")
	if err != nil {
		return "", fmt.Errorf("build run review URL: %w", err)
	}
	return runURL + "?" + url.Values{
		"repo_url":    {pr.GetBase().GetRepo().GetHTMLURL()},
		"action_name": {actionName},
		"branch":      {pr.GetHead().GetRef()},
		"env_preset":  {"AGENT_REVIEW"},
	}.Encode(), nil
}

func reviewFooter(pr *github.PullRequest) string {
	// Keep this in sync with the env var name in ci_runner_env.
	actionName := os.Getenv("BUILDBUDDY_ACTION_NAME")
	if actionName == "" {
		return "<sub>Reviewed with [`bb agent review`](" + reviewDocsURL + ")</sub>"
	}
	footer := "<sub>Reviewed with [`bb agent review`](" + reviewDocsURL + ") on BuildBuddy Workflows</sub>"
	runURL, err := runReviewURL(pr, actionName)
	if err != nil {
		log.Warnf("Omitting re-run link from review: %s", err)
		return footer
	}
	return "**[Re-run review](" + runURL + ")**\n\n" + footer
}

// structureReview has the agent convert a free-form review into reviewJSON.
func structureReview(ctx context.Context, reviewText string) (*reviewJSON, error) {
	buf := &bytes.Buffer{}
	err := agent.Run(ctx, &agentutil.RunRequest{
		Agent:           *agentflags.Agent,
		Model:           *agentflags.Model,
		ReasoningEffort: *agentflags.Effort,
		Prompt:          editPrompt + reviewText,
		CodexSandbox:    agentutil.SandboxReadOnly,
		Output:          buf,
		Progress:        io.Discard,
	})
	if err != nil {
		return nil, err
	}

	// Strip accidental markdown fences.
	var cleanLines []string
	for line := range strings.SplitSeq(buf.String(), "\n") {
		if !strings.HasPrefix(line, "```") {
			cleanLines = append(cleanLines, line)
		}
	}

	review := &reviewJSON{}
	if err := json.Unmarshal([]byte(strings.Join(cleanLines, "\n")), review); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return review, nil
}

// placeComment returns where to post c inline. GitHub only accepts inline
// comments on lines in the diff, so a comment on any other line moves to the
// nearest diff line in the same file, or else to the PR's first diff line.
// ok is false only if the diff has no lines.
func placeComment(c reviewComment, validLines map[fileLine]bool) (loc fileLine, ok bool) {
	want := fileLine{c.File, c.Line}
	if validLines[want] {
		return want, true
	}
	var nearest, first fileLine
	for l := range validLines {
		if l.file == c.File {
			d, best := abs(l.line-c.Line), abs(nearest.line-c.Line)
			if nearest.file == "" || d < best || (d == best && l.line < nearest.line) {
				nearest = l
			}
		}
		if first.file == "" || l.file < first.file || (l.file == first.file && l.line < first.line) {
			first = l
		}
	}
	if nearest.file != "" {
		return nearest, true
	}
	return first, first.file != ""
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// diffLines returns the set of lines in the new version of each file that
// appear in the diff hunks.
func diffLines(diff string) map[fileLine]bool {
	validLines := make(map[fileLine]bool)
	var currentFile string
	newLine := 0
	scanner := bufio.NewScanner(strings.NewReader(diff))
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		l := scanner.Text()
		if m := fileRe.FindStringSubmatch(l); m != nil {
			currentFile = m[1]
			newLine = 0
		} else if m := hunkRe.FindStringSubmatch(l); m != nil {
			n, _ := strconv.Atoi(m[1])
			newLine = n - 1
		} else if currentFile != "" {
			switch {
			case strings.HasPrefix(l, "-"):
				// deleted line — don't advance
			case strings.HasPrefix(l, "+"), strings.HasPrefix(l, " "):
				newLine++
				validLines[fileLine{currentFile, newLine}] = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Warnf("Diff scanner error (some inline comments may be demoted to body): %s", err)
	}
	return validLines
}

func newGHClient() (*github.Client, error) {
	token := ""
	for _, env := range []string{"REPO_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"} {
		if token = os.Getenv(env); token != "" {
			break
		}
	}
	if token == "" {
		return nil, fmt.Errorf("no GitHub token found; set REPO_TOKEN, GH_TOKEN, or GITHUB_TOKEN")
	}
	// Propagate to GH_TOKEN so the gh CLI (used by the review agent) can authenticate.
	os.Setenv("GH_TOKEN", token)
	return github.NewClient(nil).WithAuthToken(token), nil
}

func fetchRepoInfo() (owner, repo, branch string, err error) {
	repoRaw, err := run("gh", "repo", "view", "--json", "nameWithOwner")
	if err != nil {
		return "", "", "", fmt.Errorf("get repo info: %w", err)
	}
	var repoInfo struct {
		NameWithOwner string `json:"nameWithOwner"`
	}
	if err := json.Unmarshal([]byte(repoRaw), &repoInfo); err != nil {
		return "", "", "", fmt.Errorf("parse repo info: %w", err)
	}
	owner, repo, ok := strings.Cut(repoInfo.NameWithOwner, "/")
	if !ok {
		return "", "", "", fmt.Errorf("unexpected nameWithOwner format: %q", repoInfo.NameWithOwner)
	}

	branch, err = run("git", "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return "", "", "", fmt.Errorf("get current branch: %w", err)
	}
	return owner, repo, branch, nil
}

// postReview posts the review to the PR, or prints it when --dry_run is set.
func postReview(ctx context.Context, gh *github.Client, owner, repo string, pr *github.PullRequest, body string, comments []*github.DraftReviewComment) (int, error) {
	prNumber := pr.GetNumber()
	footer := reviewFooter(pr)
	req := &github.PullRequestReviewRequest{
		CommitID: new(pr.GetHead().GetSHA()),
		Event:    new("COMMENT"),
		Body:     new(body + "\n\n" + footer),
		Comments: comments,
	}
	if *dryRun {
		payload, err := json.MarshalIndent(req, "", "  ")
		if err != nil {
			return -1, fmt.Errorf("marshal review payload: %w", err)
		}
		fmt.Println("Dry run — payload that would be posted:")
		fmt.Println(string(payload))
		return 0, nil
	}

	log.Printf("Posting review to GitHub...")
	posted, _, err := gh.PullRequests.CreateReview(ctx, owner, repo, prNumber, req)
	if err != nil && len(comments) > 0 {
		// Inline POST failed (likely invalid line numbers) — fold findings into body and retry.
		log.Warnf("Posting inline comments failed (%s); retrying as a single body comment.", err)
		var fallbackBody strings.Builder
		fallbackBody.WriteString(body + "\n\n### Inline findings\n")
		for _, c := range comments {
			fmt.Fprintf(&fallbackBody, "- `%s:%d` — %s\n", c.GetPath(), c.GetLine(), c.GetBody())
		}
		fallbackBody.WriteString("\n" + footer)
		req.Comments = nil
		req.Body = new(fallbackBody.String())
		posted, _, err = gh.PullRequests.CreateReview(ctx, owner, repo, prNumber, req)
	}
	if err != nil {
		return -1, fmt.Errorf("post review: %w", err)
	}
	if url := posted.GetHTMLURL(); url != "" {
		log.Printf("Review posted to: %s", url)
	}
	return 0, nil
}

func run(args ...string) (string, error) {
	cmd := exec.Command(args[0], args[1:]...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%v: %w\n%s", args, err, stderr.String())
	}
	return strings.TrimRight(stdout.String(), "\n"), nil
}
