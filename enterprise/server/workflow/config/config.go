package config

import (
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"gopkg.in/yaml.v2"
)

// === IMPORTANT ===
// If you edit this file, also update docs/workflows-config.md
// =================

const (
	// FilePath is the path where we can expect to locate the BuildBuddyConfig
	// YAML contents, relative to the repository root.
	FilePath = "buildbuddy.yaml"

	// KytheActionName is the name used for actions automatically
	// run by us if code search is enabled.
	KytheActionName = "Generate CodeSearch Index"
)

type BuildBuddyConfig struct {
	Actions []*Action `yaml:"actions"`
}

type Action struct {
	Name              string            `yaml:"name"`
	Triggers          *Triggers         `yaml:"triggers"`
	OS                string            `yaml:"os"`
	Arch              string            `yaml:"arch"`
	Pool              string            `yaml:"pool"`
	SelfHosted        bool              `yaml:"self_hosted"`
	ContainerImage    string            `yaml:"container_image"`
	ResourceRequests  ResourceRequests  `yaml:"resource_requests"`
	User              string            `yaml:"user"`
	GitCleanExclude   []string          `yaml:"git_clean_exclude"`
	GitFetchFilters   []string          `yaml:"git_fetch_filters"`
	GitFetchDepth     int               `yaml:"git_fetch_depth"`
	BazelWorkspaceDir string            `yaml:"bazel_workspace_dir"`
	Env               map[string]string `yaml:"env"`
	BazelCommands     []string          `yaml:"bazel_commands"`
	Timeout           *time.Duration    `yaml:"timeout"`
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
}

func (t *Triggers) GetPullRequestTrigger() *PullRequestTrigger {
	if t.PullRequest == nil {
		return &PullRequestTrigger{}
	}
	return t.PullRequest
}

type PushTrigger struct {
	Branches []string `yaml:"branches"`
}

type PullRequestTrigger struct {
	Branches []string `yaml:"branches"`
	// NOTE: If nil, defaults to true.
	MergeWithBase *bool `yaml:"merge_with_base"`
}

func (t *PullRequestTrigger) GetMergeWithBase() bool {
	return t.MergeWithBase == nil || *t.MergeWithBase
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

func KytheIndexingAction(targetRepoDefaultBranch string) *Action {
	var pushTriggerBranches []string
	if targetRepoDefaultBranch != "" {
		pushTriggerBranches = append(pushTriggerBranches, targetRepoDefaultBranch)
	}

	cmd := "build --override_repository kythe_release=$KYTHE_DIR"
	cmd += " --remote_cache=" + cache_api_url.String() + " --experimental_remote_cache_compression"
	cmd += " //..."

	return &Action{
		Name: KytheActionName,
		Triggers: &Triggers{
			Push: &PushTrigger{Branches: pushTriggerBranches},
		},
		ContainerImage: `ubuntu-20.04`,
		ResourceRequests: ResourceRequests{
			CPU:    "8",
			Memory: "16GB",
			Disk:   "100GB",
		},
		BazelCommands: []string{cmd},
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
				BazelCommands: []string{"test //..."},
			},
		},
	}
}

// MatchesAnyTrigger returns whether the action is triggered by the event
// published to the given branch.
func MatchesAnyTrigger(action *Action, event, branch string) bool {
	if action.Triggers == nil {
		return false
	}
	if pushCfg := action.Triggers.Push; pushCfg != nil && event == webhook_data.EventName.Push {
		return matchesAnyBranch(pushCfg.Branches, branch)
	}

	if prCfg := action.Triggers.PullRequest; prCfg != nil && event == webhook_data.EventName.PullRequest {
		return matchesAnyBranch(prCfg.Branches, branch)
	}
	return false
}

// MatchesAnyActionName returns whether the given action matches any of the
// given action names.
func MatchesAnyActionName(action *Action, names []string) bool {
	for _, name := range names {
		if action.Name == name {
			return true
		}
	}
	return false
}

func matchesAnyBranch(branches []string, branch string) bool {
	for _, b := range branches {
		if b == "*" {
			return true
		}
		if b == branch {
			return true
		}
		if b == "gh-readonly-queue/*" && strings.HasPrefix(branch, "gh-readonly-queue/") {
			return true
		}
	}
	return false
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
