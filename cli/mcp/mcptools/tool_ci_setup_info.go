package mcptools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
)

const (
	ciProviderBuildBuddyWorkflows = "buildbuddy_workflows"
	ciProviderGitHubActions       = "github_actions"
	ciProviderBuildkite           = "buildkite"
	ciProviderGitLabCI            = "gitlab_ci"
	ciProviderCircleCI            = "circleci"
	ciProviderJenkins             = "jenkins"
	ciProviderAzurePipelines      = "azure_pipelines"

	ciRunnerRoleWorkflowNote  = "# Filter to role=\"CI_RUNNER\" and workflow_action_name=\"<action_name>\" to see \"parent\" workflow invocations, but note that these invocations don't have rich timing profile data available."
	ciChildInvocationRoleNote = "# Filter to role=\"CI\" to see child bazel invocations (and apply additional filters as needed to disambiguate between workflows, e.g. pattern=\"//...\")"
)

var (
	githubSCPLikeRemotePattern = regexp.MustCompile(`^(?:ssh://)?git@github\.com[:/]([^/]+)/([^/]+?)(?:\.git)?/?$`)
	workflowActionNamePattern  = regexp.MustCompile(`(?m)^\s*-\s*name:\s*(.+?)\s*$`)
)

type githubRequiredChecksResult struct {
	Repo                   string
	DefaultBranch          string
	RequiredChecks         []string
	RequiredChecksByBranch map[string][]string
	Source                 string
}

type githubRepoInfo struct {
	Owner string
	Repo  string
}

type githubRepoMetadataResponse struct {
	DefaultBranch string `json:"default_branch"`
}

type githubBranchProtectionResponse struct {
	RequiredStatusChecks *githubRequiredStatusChecks `json:"required_status_checks"`
}

type githubRequiredStatusChecks struct {
	Contexts []string                     `json:"contexts"`
	Checks   []*githubRequiredCheckRecord `json:"checks"`
}

type githubRequiredCheckRecord struct {
	Context string `json:"context"`
}

func (s *Service) ciSetupInfo(ctx context.Context, _ map[string]any) (any, error) {
	repoRoot, err := storage.RepoRootPath()
	if err != nil {
		return map[string]any{
			"repo_root":                   "",
			"detected_ci_providers":       []string{},
			"detected_files":              []string{},
			"buildbuddy_workflows_in_use": false,
			"ci_detection_error":          err.Error(),
			"hint":                        "No supported CI providers were detected. Other CI configuration files may still be present in this repo.",
			"recommendation":              "Could not determine repo CI setup. If this repo uses BuildBuddy workflows, use buildbuddy MCP tools to inspect invocations directly.",
		}, nil
	}

	providerFiles := map[string][]string{
		ciProviderBuildBuddyWorkflows: detectCIFiles(repoRoot,
			"buildbuddy.yaml",
		),
		ciProviderGitHubActions: detectCIFiles(repoRoot,
			".github/workflows/*.yaml",
			".github/workflows/*.yml",
		),
		ciProviderBuildkite: detectCIFiles(repoRoot,
			".buildkite/pipeline.yaml",
			".buildkite/pipeline.yml",
			".buildkite/pipeline.json",
		),
		ciProviderGitLabCI: detectCIFiles(repoRoot, ".gitlab-ci.yml", ".gitlab-ci.yaml"),
		ciProviderCircleCI: detectCIFiles(repoRoot,
			".circleci/config.yml",
			".circleci/config.yaml",
		),
		ciProviderJenkins:        detectCIFiles(repoRoot, "Jenkinsfile"),
		ciProviderAzurePipelines: detectCIFiles(repoRoot, "azure-pipelines.yml", "azure-pipelines.yaml"),
	}

	detectedProviders := make([]string, 0, len(providerFiles))
	detectedFiles := make([]string, 0, 8)
	for provider, files := range providerFiles {
		if len(files) == 0 {
			continue
		}
		detectedProviders = append(detectedProviders, provider)
		detectedFiles = append(detectedFiles, files...)
	}
	sort.Strings(detectedProviders)
	sort.Strings(detectedFiles)
	buildbuddyWorkflowsInUse := len(providerFiles[ciProviderBuildBuddyWorkflows]) > 0
	buildbuddyYAMLPath := ""
	buildbuddyYAML := ""
	buildbuddyWorkflowActionNames := []string{}
	if buildbuddyWorkflowsInUse {
		buildbuddyYAMLPath = preferredBuildBuddyYAMLPath(providerFiles[ciProviderBuildBuddyWorkflows])
		if buildbuddyYAMLPath != "" {
			content, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(buildbuddyYAMLPath)))
			if err != nil {
				buildbuddyYAML = fmt.Sprintf("could not read %q: %s", buildbuddyYAMLPath, err)
			} else {
				buildbuddyYAMLContent := string(content)
				buildbuddyWorkflowActionNames = extractBuildBuddyWorkflowActionNames(buildbuddyYAMLContent)
				buildbuddyYAML = appendCINotesToBuildBuddyYAML(buildbuddyYAMLContent)
			}
		}
	}
	githubLookupStatus := "not_attempted"
	githubLookupError := ""
	githubRequiredChecksSource := ""
	githubRepo := ""
	githubDefaultBranch := ""
	githubRequiredChecks := []string{}
	githubRequiredChecksByBranch := map[string][]string{}
	if result, err := bestEffortGitHubRequiredChecks(ctx, repoRoot); err != nil {
		githubLookupStatus = "error"
		githubLookupError = err.Error()
	} else if result == nil {
		githubLookupStatus = "skipped"
	} else {
		githubLookupStatus = "ok"
		githubRepo = result.Repo
		githubDefaultBranch = result.DefaultBranch
		githubRequiredChecks = append([]string{}, result.RequiredChecks...)
		githubRequiredChecksByBranch = result.RequiredChecksByBranch
		githubRequiredChecksSource = result.Source
	}
	requiredBuildBuddyWorkflowActions := intersectStrings(buildbuddyWorkflowActionNames, githubRequiredChecks)
	missingRequiredChecksInBuildBuddyYAML := differenceStrings(githubRequiredChecks, buildbuddyWorkflowActionNames)

	recommendation := "No known CI config files were detected."
	if buildbuddyWorkflowsInUse {
		recommendation = "BuildBuddy workflows are in use - use buildbuddy MCP tools to answer questions about CI invocations for this repo."
	} else if len(detectedProviders) > 0 {
		recommendation = "Non-BuildBuddy CI config files were detected. If this CI uploads builds to BuildBuddy, use buildbuddy MCP tools for invocation analysis; otherwise inspect the CI config files directly."
	}
	hint := ""
	if len(detectedProviders) == 0 {
		hint = "No supported CI providers were detected. Other CI configuration files may still be present in this repo."
	}
	response := map[string]any{
		"repo_root":                                     repoRoot,
		"detected_ci_providers":                         detectedProviders,
		"detected_files":                                detectedFiles,
		"provider_files":                                providerFiles,
		"buildbuddy_workflows_in_use":                   buildbuddyWorkflowsInUse,
		"buildbuddy_yaml_path":                          buildbuddyYAMLPath,
		"buildbuddy_yaml":                               buildbuddyYAML,
		"buildbuddy_workflow_actions":                   buildbuddyWorkflowActionNames,
		"github_lookup_status":                          githubLookupStatus,
		"github_lookup_error":                           githubLookupError,
		"github_repo":                                   githubRepo,
		"github_default_branch":                         githubDefaultBranch,
		"github_required_ci_checks":                     githubRequiredChecks,
		"github_required_checks_by_branch":              githubRequiredChecksByBranch,
		"github_required_checks_source":                 githubRequiredChecksSource,
		"required_buildbuddy_workflow_actions":          requiredBuildBuddyWorkflowActions,
		"missing_required_ci_checks_in_buildbuddy_yaml": missingRequiredChecksInBuildBuddyYAML,
		"recommendation":                                recommendation,
	}
	if hint != "" {
		response["hint"] = hint
	}
	return response, nil
}

func bestEffortGitHubRequiredChecks(ctx context.Context, repoRoot string) (*githubRequiredChecksResult, error) {
	repo, err := detectGitHubRepo(ctx, repoRoot)
	if err != nil {
		return nil, err
	}
	if repo == nil {
		return nil, nil
	}

	repoEndpoint := fmt.Sprintf("repos/%s/%s", repo.Owner, repo.Repo)
	repoBody, err := runGHAPI(ctx, repoRoot, repoEndpoint)
	if err != nil {
		return nil, err
	}
	repoMetadata := &githubRepoMetadataResponse{}
	if err := json.Unmarshal(repoBody, repoMetadata); err != nil {
		return nil, fmt.Errorf("decode gh response for %q: %w", repoEndpoint, err)
	}
	if repoMetadata.DefaultBranch == "" {
		return nil, fmt.Errorf("gh response for %q did not include default_branch", repoEndpoint)
	}

	protectionEndpoint := fmt.Sprintf(
		"repos/%s/%s/branches/%s/protection",
		repo.Owner,
		repo.Repo,
		url.PathEscape(repoMetadata.DefaultBranch),
	)
	protectionBody, err := runGHAPI(ctx, repoRoot, protectionEndpoint)
	if err != nil {
		if strings.Contains(err.Error(), "HTTP 404") {
			return &githubRequiredChecksResult{
				Repo:                   repo.Owner + "/" + repo.Repo,
				DefaultBranch:          repoMetadata.DefaultBranch,
				RequiredChecks:         []string{},
				RequiredChecksByBranch: map[string][]string{repoMetadata.DefaultBranch: {}},
				Source:                 "gh api " + protectionEndpoint,
			}, nil
		}
		return nil, err
	}
	protection := &githubBranchProtectionResponse{}
	if err := json.Unmarshal(protectionBody, protection); err != nil {
		return nil, fmt.Errorf("decode gh response for %q: %w", protectionEndpoint, err)
	}
	requiredChecks := requiredChecksFromBranchProtection(protection)
	return &githubRequiredChecksResult{
		Repo:          repo.Owner + "/" + repo.Repo,
		DefaultBranch: repoMetadata.DefaultBranch,
		RequiredChecksByBranch: map[string][]string{
			repoMetadata.DefaultBranch: requiredChecks,
		},
		RequiredChecks: requiredChecks,
		Source:         "gh api " + protectionEndpoint,
	}, nil
}

func detectGitHubRepo(ctx context.Context, repoRoot string) (*githubRepoInfo, error) {
	remoteURL, err := gitRemoteOriginURL(ctx, repoRoot)
	if err != nil {
		return nil, err
	}
	owner, repo, ok := parseGitHubOwnerRepo(remoteURL)
	if !ok {
		return nil, nil
	}
	return &githubRepoInfo{Owner: owner, Repo: repo}, nil
}

func gitRemoteOriginURL(ctx context.Context, repoRoot string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "config", "--get", "remote.origin.url")
	cmd.Dir = repoRoot
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if strings.TrimSpace(stderr.String()) == "" {
				return "", nil
			}
		}
		return "", fmt.Errorf("read git remote.origin.url: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

func parseGitHubOwnerRepo(remoteURL string) (string, string, bool) {
	trimmed := strings.TrimSpace(remoteURL)
	if trimmed == "" {
		return "", "", false
	}
	if matches := githubSCPLikeRemotePattern.FindStringSubmatch(trimmed); len(matches) == 3 {
		return matches[1], strings.TrimSuffix(matches[2], ".git"), true
	}
	parsedURL, err := url.Parse(trimmed)
	if err != nil {
		return "", "", false
	}
	if !strings.EqualFold(parsedURL.Hostname(), "github.com") {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], strings.TrimSuffix(parts[1], ".git"), true
}

func runGHAPI(ctx context.Context, repoRoot, endpoint string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "gh", "api", "-H", "Accept: application/vnd.github+json", endpoint)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GH_PROMPT_DISABLED=1", "GH_NO_UPDATE_NOTIFIER=1")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return nil, fmt.Errorf("gh CLI not found while querying %q", endpoint)
		}
		return nil, fmt.Errorf("gh api %q: %w: %s", endpoint, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func requiredChecksFromBranchProtection(protection *githubBranchProtectionResponse) []string {
	if protection == nil || protection.RequiredStatusChecks == nil {
		return []string{}
	}
	seen := make(map[string]struct{}, len(protection.RequiredStatusChecks.Contexts)+len(protection.RequiredStatusChecks.Checks))
	checks := make([]string, 0, len(protection.RequiredStatusChecks.Contexts)+len(protection.RequiredStatusChecks.Checks))
	for _, contextName := range protection.RequiredStatusChecks.Contexts {
		contextName = strings.TrimSpace(contextName)
		if contextName == "" {
			continue
		}
		if _, ok := seen[contextName]; ok {
			continue
		}
		seen[contextName] = struct{}{}
		checks = append(checks, contextName)
	}
	for _, check := range protection.RequiredStatusChecks.Checks {
		if check == nil {
			continue
		}
		contextName := strings.TrimSpace(check.Context)
		if contextName == "" {
			continue
		}
		if _, ok := seen[contextName]; ok {
			continue
		}
		seen[contextName] = struct{}{}
		checks = append(checks, contextName)
	}
	sort.Strings(checks)
	return checks
}

func extractBuildBuddyWorkflowActionNames(buildbuddyYAML string) []string {
	matches := workflowActionNamePattern.FindAllStringSubmatch(buildbuddyYAML, -1)
	seen := make(map[string]struct{}, len(matches))
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		name := strings.TrimSpace(match[1])
		name = strings.Trim(name, "\"'")
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func intersectStrings(left, right []string) []string {
	if len(left) == 0 || len(right) == 0 {
		return []string{}
	}
	intersection := slices.Collect(set.Intersection(set.From(left...), set.From(right...)))
	sort.Strings(intersection)
	return intersection
}

func differenceStrings(left, right []string) []string {
	if len(left) == 0 {
		return []string{}
	}
	diff := slices.Collect(set.Difference(set.From(left...), set.From(right...)))
	sort.Strings(diff)
	return diff
}

func preferredBuildBuddyYAMLPath(paths []string) string {
	for _, p := range paths {
		if p == "buildbuddy.yaml" {
			return p
		}
	}
	return ""
}

func appendCINotesToBuildBuddyYAML(content string) string {
	content = strings.TrimRight(content, "\n")
	if content == "" {
		return ciRunnerRoleWorkflowNote + "\n" + ciChildInvocationRoleNote + "\n"
	}
	return content + "\n" + ciRunnerRoleWorkflowNote + "\n" + ciChildInvocationRoleNote + "\n"
}

func detectCIFiles(repoRoot string, patterns ...string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		matches, err := filepath.Glob(filepath.Join(repoRoot, pattern))
		if err != nil {
			continue
		}
		for _, match := range matches {
			relativePath, err := filepath.Rel(repoRoot, match)
			if err != nil {
				continue
			}
			relativePath = filepath.ToSlash(relativePath)
			if _, ok := seen[relativePath]; ok {
				continue
			}
			seen[relativePath] = struct{}{}
			out = append(out, relativePath)
		}
	}
	sort.Strings(out)
	return out
}
