package review

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
usage: bb agent review [ <pr> ] [ --dry_run ] [ --force ]

Reviews an open GitHub pull request, then posts the findings as PR review
comments.

  <pr>  Optional. The PR number to review. With no number, the PR for the
        current branch is reviewed.

Requires gh and a GitHub token in REPO_TOKEN, GH_TOKEN, or GITHUB_TOKEN.

Drafts and PRs that already have a bot review are skipped; pass --force to
review them anyway.

Examples:
  bb agent review
  bb agent review 13315
  bb agent review --dry_run
`

// Flags holds the flags unique to this subcommand. The flags shared by all
// `bb agent` subcommands are registered on it by the agent package.
var (
	Flags = flag.NewFlagSet("review", flag.ContinueOnError)

	dryRun = Flags.Bool("dry_run", false, "Print the review payload without posting it.")
	force  = Flags.Bool("force", false, "Review the PR even if it is a draft or already has a bot review.")
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

const reviewPrompt = "Review PR #%d. /review\n\nFor every finding, include the exact file path and line number in the format `path/to/file.go:42` so findings can be posted as inline GitHub comments.\n\nReview by reading the diff and the surrounding source. Do not build, run, or test the code — that can be slow and is not the goal of this review. Do not edit any files, and do not commit, push, or post anything to GitHub. Only report the findings."

const parsePrompt = `Parse the code review below and output ONLY a valid JSON object — no markdown fences, no explanation.

Schema:
{
  "summary": "<concise summary of the diff as markdown, max ~3 sentences but aim for fewer>",
  "comments": [
    {
      "file": "<file path relative to repo root, e.g. server/foo.go>",
      "line": <integer — the line number in the NEW version of the file>,
      "body": "<full comment text as markdown>"
    }
  ]
}

Rules:
- Include a comment entry for every unique finding that mentions a file path, even if the line number is approximate — use the nearest relevant line you can infer from context.
  Avoid including redundant comments; instead of making the same comment on every line where a particular issue occurs,
  you can say things like "(same comment applies to ...)" or "... (here and below)."
- Findings with no file reference at all go into "summary".
- File paths must be relative (no leading slash, no absolute paths).
- Remove any footnote-style numeric references such as "(#1)" or "(#2)" from comment bodies and the summary — GitHub interprets these as issue/PR links.
- Omit comments that only praise, affirm, or acknowledge a change without raising an actionable concern or suggesting an improvement (e.g. "good cleanup", "no callers found — safe to remove", "looks correct").
- Avoid "introductory" or summary phrases with labels or categorizations like "Performance improvement: strings.Split allocates. Use strings.SplitSeq instead.". Just write the comment directly, like "strings.Split allocates. Use strings.SplitSeq instead."
- The summary section should not summarize any of the individual comments. It should only explain the purpose of the diff and findings with no file reference.
- Output ONLY the JSON object. No other text before or after it.

REVIEW:
`

var (
	hunkRe = regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@`)
	fileRe = regexp.MustCompile(`^\+\+\+ b/(.+)$`)
)

type reviewComment struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Body string `json:"body"`
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
	if len(args) > 1 {
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

	owner, repo, err := fetchRepoInfo()
	if err != nil {
		return -1, err
	}
	prNumber, err := findPRNumber(args)
	if err != nil {
		return -1, err
	}
	log.Printf("Fetching PR info...")
	pr, _, err := gh.PullRequests.Get(ctx, owner, repo, prNumber)
	if err != nil {
		return -1, fmt.Errorf("get PR #%d: %w", prNumber, err)
	}
	if pr.GetState() != "open" {
		return -1, fmt.Errorf("PR #%d is %s", prNumber, pr.GetState())
	}

	headSHA := pr.GetHead().GetSHA()
	headShort := headSHA
	if len(headShort) > 8 {
		headShort = headShort[:8]
	}
	log.Printf("PR #%d on %s/%s  (head: %s, base: %s)", pr.GetNumber(), owner, repo, headShort, pr.GetBase().GetRef())

	// A PR is reviewed once, when it's ready for review, so skip drafts and PRs
	// that already have a bot review to avoid spamming them with reviews.
	if !*force {
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
	reviewRsp, err := agent.Run(ctx, &agentutil.RunRequest{
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
	})
	if err != nil {
		return -1, fmt.Errorf("error running review agent: %w", err)
	}
	reviewText := reviewRsp.Output

	// Have the agent convert the review to structured JSON. If that fails,
	// post the unstructured review as a single body comment.
	log.Printf("Structuring review output into JSON...")
	review, err := structureReview(ctx, reviewText)
	if err != nil {
		log.Warnf("Failed to structure review (%s); posting it as a single body comment.", err)
		return postReview(ctx, gh, owner, repo, pr.GetNumber(), headSHA, reviewText, nil)
	}
	log.Printf("    Found %d candidate line-level comment(s).", len(review.Comments))

	// GitHub only accepts inline comments on lines present in the diff hunks.
	log.Printf("Fetching PR diff to validate line numbers...")
	diff, _, err := gh.PullRequests.GetRaw(ctx, owner, repo, pr.GetNumber(), github.RawOptions{Type: github.Diff})
	if err != nil {
		return -1, fmt.Errorf("fetch PR diff: %w", err)
	}
	validLines := diffLines(diff)

	// Partition comments into inline and body overflow.
	var inlineComments []*github.DraftReviewComment
	var overflowLines []string
	for _, c := range review.Comments {
		if validLines[fileLine{c.File, c.Line}] {
			inlineComments = append(inlineComments, &github.DraftReviewComment{
				Path: new(c.File),
				Line: new(c.Line),
				Side: new("RIGHT"),
				Body: new(c.Body),
			})
		} else {
			overflowLines = append(overflowLines, fmt.Sprintf("- `%s:%d` — %s", c.File, c.Line, c.Body))
		}
	}

	fullBody := review.Summary
	if len(overflowLines) > 0 {
		fullBody += "\n\n### Additional findings (lines outside the diff)\n" + strings.Join(overflowLines, "\n")
	}
	log.Printf("    %d inline comment(s), %d folded into body.", len(inlineComments), len(overflowLines))

	return postReview(ctx, gh, owner, repo, pr.GetNumber(), headSHA, fullBody, inlineComments)
}

// shouldSkip reports whether the PR is a draft or already has a bot review.
func shouldSkip(ctx context.Context, gh *github.Client, owner, repo string, pr *github.PullRequest) (bool, error) {
	if pr.GetDraft() {
		log.Printf("PR #%d is a draft — skipping (pass --force to override).", pr.GetNumber())
		return true, nil
	}
	log.Printf("Checking for existing reviews...")
	reviews, _, err := gh.PullRequests.ListReviews(ctx, owner, repo, pr.GetNumber(), nil)
	if err != nil {
		return false, fmt.Errorf("fetch existing reviews: %w", err)
	}
	for _, r := range reviews {
		if r.GetUser().GetType() == "Bot" {
			log.Printf("PR #%d already has a bot review — skipping (pass --force to override).", pr.GetNumber())
			return true, nil
		}
	}
	return false, nil
}

// structureReview has the agent convert a free-form review into reviewJSON.
func structureReview(ctx context.Context, reviewText string) (*reviewJSON, error) {
	rsp, err := agent.Run(ctx, &agentutil.RunRequest{
		Agent:           *agentflags.Agent,
		Model:           *agentflags.Model,
		ReasoningEffort: *agentflags.Effort,
		Prompt:          parsePrompt + reviewText,
	})
	if err != nil {
		return nil, err
	}

	// Strip accidental markdown fences.
	var cleanLines []string
	for line := range strings.SplitSeq(rsp.Output, "\n") {
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

// fetchRepoInfo returns the owner and name of the base repo. PRs belong to the
// base repo even when they are opened from a fork.
func fetchRepoInfo() (owner, repo string, err error) {
	repoRaw, err := run("gh", "repo", "view", "--json", "nameWithOwner")
	if err != nil {
		return "", "", fmt.Errorf("get repo info: %w", err)
	}
	var repoInfo struct {
		NameWithOwner string `json:"nameWithOwner"`
	}
	if err := json.Unmarshal([]byte(repoRaw), &repoInfo); err != nil {
		return "", "", fmt.Errorf("parse repo info: %w", err)
	}
	owner, repo, ok := strings.Cut(repoInfo.NameWithOwner, "/")
	if !ok {
		return "", "", fmt.Errorf("unexpected nameWithOwner format: %q", repoInfo.NameWithOwner)
	}
	return owner, repo, nil
}

// findPRNumber resolves which PR to review: an explicitly given number, else
// the PR that gh associates with the current branch.
func findPRNumber(args []string) (int, error) {
	if len(args) == 1 {
		n, err := strconv.Atoi(strings.TrimPrefix(args[0], "#"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("%q is not a PR number", args[0])
		}
		return n, nil
	}

	prRaw, err := run("gh", "pr", "view", "--json", "number")
	if err != nil {
		return 0, fmt.Errorf("no PR found for the current branch; pass a PR number explicitly: %w", err)
	}
	var prInfo struct {
		Number int `json:"number"`
	}
	if err := json.Unmarshal([]byte(prRaw), &prInfo); err != nil {
		return 0, fmt.Errorf("parse PR info: %w", err)
	}
	if prInfo.Number <= 0 {
		return 0, fmt.Errorf("no PR found for the current branch; pass a PR number explicitly")
	}
	return prInfo.Number, nil
}

// postReview posts the review to the PR, or prints it when --dry_run is set.
func postReview(ctx context.Context, gh *github.Client, owner, repo string, prNumber int, headSHA, body string, comments []*github.DraftReviewComment) (int, error) {
	req := &github.PullRequestReviewRequest{
		CommitID: new(headSHA),
		Event:    new("COMMENT"),
		Body:     new(body),
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
