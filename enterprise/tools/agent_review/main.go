// review-pr — Run `claude /review`, then post findings as GitHub PR review comments.
//
// Usage:
//
//	bazel run //tools/agent_review              # review current branch's open PR
//	bazel run //tools/agent_review -- --dry_run    # print payload without posting
//
// Requirements: claude, gh, git
package main

import (
	"bufio"
	"bytes"
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

type prInfo struct {
	Number      int    `json:"number"`
	HeadRefOid  string `json:"headRefOid"`
	BaseRefName string `json:"baseRefName"`
}

type repoInfo struct {
	NameWithOwner string `json:"nameWithOwner"`
}

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

type ghReview struct {
	User struct {
		Type string `json:"type"`
	} `json:"user"`
}

type inlineComment struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	Side string `json:"side"`
	Body string `json:"body"`
}

type reviewPayload struct {
	CommitID string          `json:"commit_id"`
	Event    string          `json:"event"`
	Body     string          `json:"body"`
	Comments []inlineComment `json:"comments"`
}

type reviewResponse struct {
	HTMLURL string `json:"html_url"`
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

func main() {
	flag.Parse()

	if dir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); dir != "" {
		if err := os.Chdir(dir); err != nil {
			log.Fatalf("failed to chdir to workspace: %v", err)
		}
	}

	// The GH API reads the GH_TOKEN environment variable, but the BB remote runner
	// sets the token in REPO_TOKEN.
	if token := os.Getenv("REPO_TOKEN"); token != "" {
		os.Setenv("GH_TOKEN", token)
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

	// Fetch the PR associated with the current, checked-out branch.
	log.Info("Fetching PR info...")
	prRaw, err := run("gh", "pr", "view", "--json", "number,headRefOid,baseRefName")
	if err != nil {
		log.Fatal("no open PR found for the current branch.")
	}
	var pr prInfo
	if err := json.Unmarshal([]byte(prRaw), &pr); err != nil {
		log.Fatalf("failed to parse PR info: %v", err)
	}

	repoRaw, err := run("gh", "repo", "view", "--json", "nameWithOwner")
	if err != nil {
		log.Fatalf("failed to get repo info: %v", err)
	}
	var repo repoInfo
	if err := json.Unmarshal([]byte(repoRaw), &repo); err != nil {
		log.Fatalf("failed to parse repo info: %v", err)
	}

	headShort := pr.HeadRefOid
	if len(headShort) > 8 {
		headShort = headShort[:8]
	}
	log.Infof("PR #%d on %s  (head: %s, base: %s)", pr.Number, repo.NameWithOwner, headShort, pr.BaseRefName)

	// To prevent spamming the PR with reviews, we only post a review if the PR doesn't already have a bot review.
	// Set FORCE=1 to override and post a new review.
	if os.Getenv("FORCE") != "1" {
		log.Info("Checking for existing reviews...")
		reviewsRaw, err := run("gh", "api", fmt.Sprintf("repos/%s/pulls/%d/reviews", repo.NameWithOwner, pr.Number))
		if err != nil {
			log.Fatalf("failed to fetch existing reviews: %v", err)
		}
		var reviews []ghReview
		if err := json.Unmarshal([]byte(reviewsRaw), &reviews); err != nil {
			log.Fatalf("failed to parse reviews: %v", err)
		}
		for _, r := range reviews {
			if r.User.Type == "Bot" {
				log.Infof("PR #%d already has a bot review — skipping (set FORCE=1 to override).", pr.Number)
				os.Exit(0)
			}
		}
	}

	// Have the agent generate the review.
	log.Info("Running claude /review  (this may take a minute)...")
	reviewPrompt := fmt.Sprintf(
		"Review PR #%d. /review\n\nFor every finding, include the exact file path and line number in the format `path/to/file.go:42` so findings can be posted as inline GitHub comments.",
		pr.Number,
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
		postReviewAsBodyComment(repo.NameWithOwner, pr.Number, pr.HeadRefOid, reviewText)
		return
	}

	// Strip accidental markdown fences
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
		postReviewAsBodyComment(repo.NameWithOwner, pr.Number, pr.HeadRefOid, reviewText)
		return
	}
	log.Infof("    Found %d candidate line-level comment(s).", len(review.Comments))

	// GitHub only accepts inline comments on lines present in the diff hunks.
	// Fetch the PR diff to generate a set of valid line numbers.
	log.Info("Fetching PR diff to validate line numbers...")
	diff, err := run("gh", "api",
		fmt.Sprintf("repos/%s/pulls/%d", repo.NameWithOwner, pr.Number),
		"-H", "Accept: application/vnd.github.diff")
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

	// Partition the comments into inline and body comments.
	var inlineComments []inlineComment
	var overflowLines []string

	for _, c := range review.Comments {
		if validLines[fileLine{c.File, c.Line}] {
			inlineComments = append(inlineComments, inlineComment{
				Path: c.File,
				Line: c.Line,
				Side: "RIGHT",
				Body: fmt.Sprintf("**[%s]** %s", c.Severity, c.Body),
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

	// Fallback body includes inline findings as markdown so nothing is dropped
	// if the inline POST fails, and we need to post the feedback as a single body comment.
	fullBodyFallback := fullBody
	if len(inlineComments) > 0 {
		var inlineLines []string
		for _, c := range inlineComments {
			inlineLines = append(inlineLines, fmt.Sprintf("- `%s:%d` — %s", c.Path, c.Line, c.Body))
		}
		fullBodyFallback += "\n\n### Inline findings\n" + strings.Join(inlineLines, "\n")
	}

	// Build the review payload.
	if inlineComments == nil {
		inlineComments = []inlineComment{}
	}
	payload := reviewPayload{
		CommitID: pr.HeadRefOid,
		Event:    "COMMENT",
		Body:     fullBody,
		Comments: inlineComments,
	}
	payloadBytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		log.Fatalf("failed to marshal payload: %v", err)
	}

	if *dryRun {
		fmt.Println("Dry run — payload that would be posted:")
		fmt.Println(string(payloadBytes))
		os.Exit(0)
	}

	log.Info("Payload to be posted:")
	log.Infof("%s", string(payloadBytes))

	log.Info("Posting review to GitHub...")
	responseStr, err := runStdin(string(payloadBytes), "gh", "api",
		fmt.Sprintf("repos/%s/pulls/%d/reviews", repo.NameWithOwner, pr.Number),
		"--method", "POST", "--input", "-")
	if err != nil {
		log.Info("Warning: POST with inline comments failed (likely invalid line numbers) — retrying as single body comment.")
		fallbackPayload := reviewPayload{
			CommitID: pr.HeadRefOid,
			Event:    "COMMENT",
			Body:     fullBodyFallback,
			Comments: []inlineComment{},
		}
		fallbackBytes, _ := json.Marshal(fallbackPayload)
		responseStr, err = runStdin(string(fallbackBytes), "gh", "api",
			fmt.Sprintf("repos/%s/pulls/%d/reviews", repo.NameWithOwner, pr.Number),
			"--method", "POST", "--input", "-")
		if err != nil {
			log.Fatal("gh api POST failed. Check that REPO_TOKEN has pull_requests: write permission.")
		}
	}

	log.Info("GitHub response:")
	log.Infof("%s", responseStr)

	var response reviewResponse
	if err := json.Unmarshal([]byte(responseStr), &response); err == nil && response.HTMLURL != "" {
		log.Infof("Review posted to: %s", response.HTMLURL)
	}
}

func postReviewAsBodyComment(repo string, prNumber int, headSHA, body string) {
	log.Info("Posting review as single body comment...")
	payload, _ := json.Marshal(reviewPayload{
		CommitID: headSHA,
		Event:    "COMMENT",
		Body:     body,
		Comments: []inlineComment{},
	})
	if _, err := runStdin(string(payload), "gh", "api",
		fmt.Sprintf("repos/%s/pulls/%d/reviews", repo, prNumber),
		"--method", "POST", "--input", "-"); err != nil {
		log.Fatal("gh api POST failed. Check that REPO_TOKEN has pull_requests: write permission.")
	}
	log.Info("Done: posted review as a single comment.")
	os.Exit(0)
}
