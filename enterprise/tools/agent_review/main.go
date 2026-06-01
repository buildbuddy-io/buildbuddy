// review-pr — Run `claude /review`, then post findings as GitHub PR review comments.
//
// Usage:
//
//	bazel run //enterprise/tools/agent_review              # review current branch's open PR
//	bazel run //enterprise/tools/agent_review -- --dry_run    # print payload without posting
//
// Requirements: claude, gh, git
package main

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/google/go-github/v59/github"
)

//go:embed settings.json
var settingsSrc []byte

var dryRun = flag.Bool("dry_run", false, "print payload without posting")

const parsePrompt = `Parse the code review below and output ONLY a valid JSON object — no markdown fences, no explanation.

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
- Remove any footnote-style numeric references such as "(#1)" or "(#2)" from comment bodies and the summary — GitHub interprets these as issue/PR links.
- Output ONLY the JSON object. No other text before or after it.

REVIEW:
`

type reviewComment struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Body     string `json:"body"`
	Severity string `json:"severity"`
}

type reviewJSON struct {
	Summary  string          `json:"summary"`
	Comments []reviewComment `json:"comments"`
}

func run(args ...string) (string, error) {
	return runStdin("", args...)
}

func runStdin(stdin string, args ...string) (string, error) {
	cmd := exec.Command(args[0], args[1:]...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		err := fmt.Errorf("%v: %w\n%s", args, err, stderr.String())
		log.Warningf("%v", err)
		return "", err
	}
	return strings.TrimRight(stdout.String(), "\n"), nil
}

func newGHClient() *github.Client {
	token := os.Getenv("REPO_TOKEN")
	if token == "" {
		token = os.Getenv("GH_TOKEN")
	}
	if token == "" {
		token = os.Getenv("GITHUB_TOKEN")
	}
	if token == "" {
		log.Fatal("no GitHub token found; set REPO_TOKEN, GH_TOKEN, or GITHUB_TOKEN")
	}
	// Propagate to GH_TOKEN so gh CLI (used by claude /review) can authenticate.
	os.Setenv("GH_TOKEN", token)
	return github.NewClient(nil).WithAuthToken(token)
}

func main() {
	flag.Parse()
	ctx := context.Background()

	for _, tool := range []string{"gh", "claude"} {
		if _, err := exec.LookPath(tool); err != nil {
			log.Fatalf("%s is not installed or not in PATH", tool)
		}
	}

	if dir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); dir != "" {
		if err := os.Chdir(dir); err != nil {
			log.Fatalf("failed to chdir to workspace: %v", err)
		}
	}

	// Copy settings.json into a temp dir and point CLAUDE_CONFIG_DIR there so
	// Claude's runtime state files (.claude.json, backups/, etc.) don't dirty
	// the tracked repo tree.
	claudeConfigDir, err := os.MkdirTemp("", "claude-config-*")
	if err != nil {
		log.Fatalf("failed to create temp Claude config dir: %v", err)
	}
	defer os.RemoveAll(claudeConfigDir)
	if err := os.WriteFile(claudeConfigDir+"/settings.json", settingsSrc, 0600); err != nil {
		log.Fatalf("failed to write Claude settings: %v", err)
	}
	os.Setenv("CLAUDE_CONFIG_DIR", claudeConfigDir)

	gh := newGHClient()

	// Fetch the PR associated with the current, checked-out branch.
	owner, repoName, branch, err := fetchRepoInfo(gh)
	if err != nil {
		log.Fatalf("failed to fetch repo info: %v", err)
	}
	log.Info("Fetching PR info...")
	prs, _, err := gh.PullRequests.List(ctx, owner, repoName, &github.PullRequestListOptions{
		Head:  owner + ":" + branch,
		State: "open",
	})
	if err != nil {
		log.Fatalf("failed to list PRs: %v", err)
	}
	if len(prs) == 0 {
		log.Fatal("no open PR found for the current branch.")
	}
	pr := prs[0]

	headSHA := pr.GetHead().GetSHA()
	headShort := headSHA
	if len(headShort) > 8 {
		headShort = headShort[:8]
	}
	log.Infof("PR #%d on %s/%s  (head: %s, base: %s)", pr.GetNumber(), owner, repoName, headShort, pr.GetBase().GetRef())

	// To prevent spamming the PR with reviews, we only post a review if the PR doesn't already have a bot review.
	// Set AGENT_REVIEW_FORCE=1 to override and post a new review.
	if os.Getenv("AGENT_REVIEW_FORCE") != "1" {
		log.Info("Checking for existing reviews...")
		reviews, _, err := gh.PullRequests.ListReviews(ctx, owner, repoName, pr.GetNumber(), nil)
		if err != nil {
			log.Fatalf("failed to fetch existing reviews: %v", err)
		}
		for _, r := range reviews {
			if r.GetUser().GetType() == "Bot" {
				log.Infof("PR #%d already has a bot review — skipping (set FORCE=1 to override).", pr.GetNumber())
				os.Exit(0)
			}
		}
	}

	// Have the agent generate the review.
	log.Info("Running claude /review  (this may take a minute)...")
	reviewPrompt := fmt.Sprintf(
		"Review PR #%d. /review\n\nFor every finding, include the exact file path and line number in the format `path/to/file.go:42` so findings can be posted as inline GitHub comments.",
		pr.GetNumber(),
	)
	reviewText, err := run("claude", "--print", reviewPrompt)
	if err != nil {
		log.Fatalf("claude /review failed: %v", err)
	}
	if strings.TrimSpace(reviewText) == "" {
		log.Fatal("review produced no output.")
	}

	// Have the agent convert the review to structured JSON.
	log.Info("Structuring review output into JSON...")
	reviewJSONStr, parseErr := runStdin(parsePrompt+reviewText, "claude", "--print", "--allowedTools", "")
	if parseErr != nil {
		log.Info("Warning: failed to parse review into JSON — posting as a single body comment.")
		postReview(ctx, gh, owner, repoName, pr.GetNumber(), headSHA, reviewText, nil)
		return
	}

	// Strip accidental markdown fences.
	var cleanLines []string
	for line := range strings.SplitSeq(reviewJSONStr, "\n") {
		if !strings.HasPrefix(line, "```") {
			cleanLines = append(cleanLines, line)
		}
	}
	reviewJSONStr = strings.Join(cleanLines, "\n")

	var review reviewJSON
	if err := json.Unmarshal([]byte(reviewJSONStr), &review); err != nil {
		log.Info("Warning: structured JSON is invalid — posting as a single body comment.")
		postReview(ctx, gh, owner, repoName, pr.GetNumber(), headSHA, reviewText, nil)
		return
	}
	log.Infof("    Found %d candidate line-level comment(s).", len(review.Comments))

	// GitHub only accepts inline comments on lines present in the diff hunks.
	// Fetch the PR diff to generate a set of valid line numbers.
	log.Info("Fetching PR diff to validate line numbers...")
	diff, _, err := gh.PullRequests.GetRaw(ctx, owner, repoName, pr.GetNumber(), github.RawOptions{Type: github.Diff})
	if err != nil {
		log.Fatalf("failed to fetch PR diff: %v", err)
	}

	type fileLine struct {
		file string
		line int
	}
	validLines := make(map[fileLine]bool)

	var currentFile string
	newLine := 0
	hunkRe := regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@`)
	fileRe := regexp.MustCompile(`^\+\+\+ b/(.+)$`)

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
		log.Warningf("diff scanner error (some inline comments may be demoted to body): %v", err)
	}

	// Partition comments into inline and body overflow.
	var inlineComments []*github.DraftReviewComment
	var overflowLines []string

	for _, c := range review.Comments {
		if validLines[fileLine{c.File, c.Line}] {
			inlineComments = append(inlineComments, &github.DraftReviewComment{
				Path: github.String(c.File),
				Line: github.Int(c.Line),
				Side: github.String("RIGHT"),
				Body: github.String(fmt.Sprintf("**[%s]** %s", c.Severity, c.Body)),
			})
		} else {
			overflowLines = append(overflowLines, fmt.Sprintf("- **[%s]** `%s:%d` — %s", c.Severity, c.File, c.Line, c.Body))
		}
	}

	fullBody := review.Summary
	if len(overflowLines) > 0 {
		fullBody += "\n\n### Additional findings (lines outside the diff)\n" + strings.Join(overflowLines, "\n")
	}

	overflowCount := len(review.Comments) - len(inlineComments)
	log.Infof("    %d inline comment(s), %d folded into body.", len(inlineComments), overflowCount)

	if *dryRun {
		req := &github.PullRequestReviewRequest{
			CommitID: github.String(headSHA),
			Event:    github.String("COMMENT"),
			Body:     github.String(fullBody),
			Comments: inlineComments,
		}
		payloadBytes, _ := json.MarshalIndent(req, "", "  ")
		fmt.Println("Dry run — payload that would be posted:")
		fmt.Println(string(payloadBytes))
		os.Exit(0)
	}

	postReview(ctx, gh, owner, repoName, pr.GetNumber(), headSHA, fullBody, inlineComments)
}

func fetchRepoInfo(gh *github.Client) (owner, repo, branch string, err error) {
	repoRaw, err := run("gh", "repo", "view", "--json", "nameWithOwner")
	if err != nil {
		return "", "", "", fmt.Errorf("failed to get repo info: %v", err)
	}
	var repoInfo struct {
		NameWithOwner string `json:"nameWithOwner"`
	}
	if err := json.Unmarshal([]byte(repoRaw), &repoInfo); err != nil {
		return "", "", "", fmt.Errorf("failed to parse repo info: %v", err)
	}
	parts := strings.SplitN(repoInfo.NameWithOwner, "/", 2)
	if len(parts) != 2 {
		return "", "", "", fmt.Errorf("unexpected nameWithOwner format: %q", repoInfo.NameWithOwner)
	}
	owner, repoName := parts[0], parts[1]

	branch, err = run("git", "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return "", "", "", fmt.Errorf("failed to get current branch: %v", err)
	}

	return owner, repoName, branch, nil
}

func postReview(ctx context.Context, gh *github.Client, owner, repo string, prNumber int, headSHA, body string, comments []*github.DraftReviewComment) {
	log.Info("Posting review to GitHub...")
	req := &github.PullRequestReviewRequest{
		CommitID: github.String(headSHA),
		Event:    github.String("COMMENT"),
		Body:     github.String(body),
		Comments: comments,
	}
	posted, _, err := gh.PullRequests.CreateReview(ctx, owner, repo, prNumber, req)
	if err != nil && len(comments) > 0 {
		// Inline POST failed (likely invalid line numbers) — fold findings into body and retry.
		log.Info("Warning: POST with inline comments failed — retrying as single body comment.")
		var fallbackBody strings.Builder
		fallbackBody.WriteString(body + "\n\n### Inline findings\n")
		for _, c := range comments {
			fallbackBody.WriteString(fmt.Sprintf("- `%s:%d` — %s\n", c.GetPath(), c.GetLine(), c.GetBody()))
		}
		req.Comments = nil
		req.Body = github.String(fallbackBody.String())
		posted, _, err = gh.PullRequests.CreateReview(ctx, owner, repo, prNumber, req)
	}
	if err != nil {
		log.Fatalf("failed to post review: %v", err)
	}
	if url := posted.GetHTMLURL(); url != "" {
		log.Infof("Review posted to: %s", url)
	}
}
