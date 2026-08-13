package config

import (
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"gopkg.in/yaml.v2"

	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

// === IMPORTANT ===
// If you edit this file, also update docs/workflows-config.md
// =================

const (
	// FilePath is the path where we can expect to locate the BuildBuddyConfig
	// YAML contents, relative to the repository root.
	FilePath = "buildbuddy.yaml"

	// CSIncrementalUpdateName is the name used for an action that sends an incremental update
	// to the codesearch indexer. This action is run automatically if codesearch is enabled.
	CSIncrementalUpdateName = "Codesearch Incremental Update"
)

type BuildBuddyConfig struct {
	Actions []*Action `yaml:"actions"`
}

type Action struct {
	Name               string            `yaml:"name"`
	Triggers           *Triggers         `yaml:"triggers"`
	OS                 string            `yaml:"os"`
	Arch               string            `yaml:"arch"`
	Pool               string            `yaml:"pool"`
	SelfHosted         bool              `yaml:"self_hosted"`
	ContainerImage     string            `yaml:"container_image"`
	ResourceRequests   ResourceRequests  `yaml:"resource_requests"`
	Priority           int               `yaml:"priority"`
	User               string            `yaml:"user"`
	GitCleanExclude    []string          `yaml:"git_clean_exclude"`
	GitFetchFilters    []string          `yaml:"git_fetch_filters"`
	GitFetchDepth      *int              `yaml:"git_fetch_depth"`
	BazelWorkspaceDir  string            `yaml:"bazel_workspace_dir"`
	Env                map[string]string `yaml:"env"`
	Visibility         string            `yaml:"visibility"`
	PlatformProperties map[string]string `yaml:"platform_properties"`
	Steps              []*rnpb.Step      `yaml:"steps"`
	Timeout            *time.Duration    `yaml:"timeout"`
	// Whether the BuildBuddy CLI (`bb`) should be used as the bazel command for this action.
	// If set, this overrides the repo-level setting.
	BazelUseCLI *bool `yaml:"bazel_use_cli"`
	// By default, if you have multiple workflow runs on the same branch, we'll cancel the old ones.
	// If AllowConcurrentRuns is set to true, we'll allow multiple runs to continue in parallel.
	AllowConcurrentRuns *bool `yaml:"allow_concurrent_runs"`

	// DEPRECATED: Used `Steps` instead
	DeprecatedBazelCommands []string `yaml:"bazel_commands"`
}

type Step struct {
	Run string `yaml:"run"`
}

func (a *Action) GetTriggers() *Triggers {
	if a.Triggers == nil {
		return &Triggers{}
	}
	return a.Triggers
}

func (a *Action) GetGitFetchFilters() []string {
	if a.GitFetchFilters == nil {
		// Default to blob:none if unspecified.
		// TODO: this seems to increase fetch time in some cases;
		// consider removing this as the default.
		return []string{"blob:none"}
	}
	return a.GitFetchFilters
}

type Triggers struct {
	Push        *PushTrigger        `yaml:"push"`
	PullRequest *PullRequestTrigger `yaml:"pull_request"`
	Schedule    *ScheduleTrigger    `yaml:"schedule"`
}

func (t *Triggers) GetPullRequestTrigger() *PullRequestTrigger {
	if t.PullRequest == nil {
		return &PullRequestTrigger{}
	}
	return t.PullRequest
}

type PushTrigger struct {
	Branches []string `yaml:"branches"`
	Tags     []string `yaml:"tags"`
}

// defaultPullRequestTypes are the pull_request actions that trigger a
// pull_request trigger when "types" is not specified. This matches the default
// behavior of GitHub Actions' pull_request trigger ("edited" is included
// because the webhook only emits it for base-branch changes).
var defaultPullRequestTypes = []string{"opened", "synchronize", "reopened", "edited"}

type PullRequestTrigger struct {
	Branches []string `yaml:"branches"`
	// Types optionally restricts the trigger to specific pull_request actions
	// (e.g. "ready_for_review"). If empty, the default set of actions is used:
	// opened, synchronize, reopened, and base-branch edits.
	Types []string `yaml:"types"`
	// NOTE: If nil, defaults to true.
	MergeWithBase *bool `yaml:"merge_with_base"`
	// If MergeWithBase is enabled, determines whether the CI runner should manually
	// merge the pushed and target branches. If this is disabled, the runner
	// will try to use the merge commit SHA provided by the CI provider as a
	// performance optimization.
	// If MergeWithBase is disabled, this does nothing.
	ForceManualMergeWithBase *bool `yaml:"force_manual_merge_with_base"`
}

func (t *PullRequestTrigger) GetMergeWithBase() bool {
	return t.MergeWithBase == nil || *t.MergeWithBase
}

func (t *PullRequestTrigger) GetForceManualMerge() bool {
	return t.GetMergeWithBase() && (t.ForceManualMergeWithBase == nil || *t.ForceManualMergeWithBase)
}

// matchesAction returns whether the trigger should fire for the given
// pull_request action. An empty action (e.g. a pull_request_review re-run that
// carries no action) only matches triggers using the default type set.
func (t *PullRequestTrigger) matchesAction(action string) bool {
	if len(t.Types) > 0 {
		return slices.Contains(t.Types, action)
	}
	return action == "" || slices.Contains(defaultPullRequestTypes, action)
}

type ScheduleTrigger struct {
	Crons []string `yaml:"crons"`
}

type ResourceRequests struct {
	// Memory is a numeric quantity of memory in bytes, or human-readable IEC
	// byte notation like "1GB" = 1024^3 bytes.
	Memory interface{} `yaml:"memory"`
	// CPU is a numeric quantity of CPU cores, or a string with a numeric
	// quantity followed by an "m"-suffix for milli-CPU.
	CPU interface{} `yaml:"cpu"`
	// Disk is a numeric quantity of disk size in bytes, or human-readable
	// IEC byte notation like "1GB" = 1024^3 bytes.
	Disk interface{} `yaml:"disk"`
}

// GetEstimatedMemory converts the memory resource request to a value compatible
// with the "EstimatedMemory" platform property.
func (r *ResourceRequests) GetEstimatedMemory() string {
	if str, ok := yamlNumberToString(r.Memory); ok {
		return str
	}
	if str, ok := r.Memory.(string); ok {
		return str
	}
	return ""
}

// GetEstimatedCPU converts the memory resource request to a value compatible
// with the "EstimatedCPU" platform property.
func (r *ResourceRequests) GetEstimatedCPU() string {
	if str, ok := yamlNumberToString(r.CPU); ok {
		return str
	}
	if str, ok := r.CPU.(string); ok {
		return str
	}
	return ""
}

func (r *ResourceRequests) GetEstimatedDisk() string {
	if str, ok := yamlNumberToString(r.Disk); ok {
		return str
	}
	if str, ok := r.Disk.(string); ok {
		return str
	}
	return ""
}

func NewConfig(r io.Reader) (*BuildBuddyConfig, error) {
	byt, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	cfg := &BuildBuddyConfig{}
	if err := yaml.Unmarshal(byt, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func sendIncrementalUpdate(apiTarget, repoURL string) string {
	// TODO(jdelfino): Remove explicit CLI installation once buildbuddy-internal/#5060 is resolved
	buf := fmt.Sprintf(`
curl -fsSL https://raw.githubusercontent.com/buildbuddy-io/buildbuddy/master/cli/install.sh | bash
git fetch --force --filter=blob:none --unshallow origin
bb index --target %s --repo-url %s`, apiTarget, repoURL)
	return buf
}

func CodesearchIncrementalUpdateAction(apiTarget *url.URL, repoURL, targetRepoDefaultBranch string) *Action {
	var pushTriggerBranches []string
	if targetRepoDefaultBranch != "" {
		pushTriggerBranches = append(pushTriggerBranches, targetRepoDefaultBranch)
	}
	return &Action{
		Name: CSIncrementalUpdateName,
		Triggers: &Triggers{
			Push: &PushTrigger{Branches: pushTriggerBranches},
		},
		ContainerImage: `ubuntu-22.04`,
		ResourceRequests: ResourceRequests{
			CPU:    "2",
			Memory: "4GB",
			Disk:   "10GB",
		},
		Steps: []*rnpb.Step{
			{
				Run: sendIncrementalUpdate(apiTarget.String(), repoURL),
			},
		},
	}
}

// GetDefault returns the default workflow config, which tests all targets when
// pushing the repo's default branch, or whenever any pull request is updated
// with a new commit.
func GetDefault(targetRepoDefaultBranch string) *BuildBuddyConfig {
	var pushTriggerBranches []string
	if targetRepoDefaultBranch != "" {
		pushTriggerBranches = append(pushTriggerBranches, targetRepoDefaultBranch)
	}
	return &BuildBuddyConfig{
		Actions: []*Action{
			{
				Name: "Test all targets",
				Triggers: &Triggers{
					Push:        &PushTrigger{Branches: pushTriggerBranches},
					PullRequest: &PullRequestTrigger{Branches: []string{"*"}},
				},
				// Note: default Bazel flags are written by the runner to ~/.bazelrc
				Steps: []*rnpb.Step{{Run: "bazel test //..."}},
			},
		},
	}
}

// MatchesAnyTrigger returns whether the action is triggered by the event
// published to the given branch or tag. prAction is the pull_request action
// (e.g. "opened", "ready_for_review") for pull_request events, and is empty for
// other events.
func MatchesAnyTrigger(action *Action, event, branch, tag, prAction string) bool {
	// If action was manually or automatically (via schedule) dispatched, always run it
	if event == webhook_data.EventName.ManualDispatch || event == webhook_data.EventName.ScheduledDispatch {
		return true
	}

	if action.Triggers == nil {
		return false
	}

	if pushCfg := action.Triggers.Push; pushCfg != nil && event == webhook_data.EventName.Push {
		if tag != "" {
			return matchesAnyPattern(pushCfg.Tags, tag)
		}
		return matchesAnyPattern(pushCfg.Branches, branch)
	}

	if prCfg := action.Triggers.PullRequest; prCfg != nil && event == webhook_data.EventName.PullRequest {
		return matchesAnyPattern(prCfg.Branches, branch) && prCfg.matchesAction(prAction)
	}
	return false
}

// MatchesAnyActionName returns whether the given action matches any of the
// given action names.
func MatchesAnyActionName(action *Action, names []string) bool {
	return slices.Contains(names, action.Name)
}

// Returns whether the given pattern matches the given text.
// The pattern is allowed to contain a single wildcard character, "*", which
// matches anything. If there is more than one wildcard, then all wildcards
// after the first one are treated as literals.
func matchesRestrictedGlob(pattern, text string) (isMatched, isNegation bool) {
	pattern, isNegation = strings.CutPrefix(pattern, "!")

	before, after, ok := strings.Cut(pattern, "*")
	if !ok {
		// No wildcard; exact match.
		return pattern == text, isNegation
	}
	prefix := before
	suffix := after
	return strings.HasPrefix(text, prefix) && strings.HasSuffix(text, suffix), isNegation
}

func matchesAnyPattern(haystack []string, needle string) bool {
	matched := false
	for _, pattern := range haystack {
		if m, isNegation := matchesRestrictedGlob(pattern, needle); m {
			matched = !isNegation
			// Keep going - last pattern wins.
		}
	}
	return matched
}

func yamlNumberToString(num interface{}) (str string, ok bool) {
	if i, ok := num.(int); ok {
		return strconv.Itoa(i), true
	}
	if f, ok := num.(float64); ok {
		return strconv.FormatFloat(f, 'e', -1, 64), true
	}
	return "", false
}
