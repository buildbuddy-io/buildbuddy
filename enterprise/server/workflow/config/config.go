package config

import (
	"fmt"
	"io"
	"path/filepath"
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
	GitFetchDepth     *int              `yaml:"git_fetch_depth"`
	BazelWorkspaceDir string            `yaml:"bazel_workspace_dir"`
	Env               map[string]string `yaml:"env"`
	BazelCommands     []string          `yaml:"bazel_commands"`
	Steps             []*rnpb.Step      `yaml:"steps"`
	Timeout           *time.Duration    `yaml:"timeout"`
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

// TODO(Maggie): Default this to false, after testing the merge commit SHA is stable.
func (t *PullRequestTrigger) GetForceManualMerge() bool {
	return t.GetMergeWithBase() && (t.ForceManualMergeWithBase == nil || *t.ForceManualMergeWithBase)
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

const kytheDownloadURL = "https://storage.googleapis.com/buildbuddy-tools/archives/kythe-v0.0.67a.tar.gz"

func checkoutKythe(dirName, downloadURL string) string {
	buf := fmt.Sprintf(`
export KYTHE_DIR="$BUILDBUDDY_CI_RUNNER_ROOT_DIR/%s"
if [ ! -d "$KYTHE_DIR" ]; then
  mkdir -p "$KYTHE_DIR"
  curl -sL "%s" | tar -xz -C "$KYTHE_DIR" --strip-components 1
fi`, dirName, downloadURL)
	return buf
}

func buildWithKythe(dirName string) string {
	return fmt.Sprintf(`
export KYTHE_DIR="$BUILDBUDDY_CI_RUNNER_ROOT_DIR/%s"
bazel --bazelrc=$KYTHE_DIR/extractors.bazelrc build --override_repository kythe_release=$KYTHE_DIR --config=buildbuddy_remote_cache //...`, dirName)
}

func prepareKytheOutputs(dirName string) string {
	buf := fmt.Sprintf(`
export KYTHE_DIR="$BUILDBUDDY_CI_RUNNER_ROOT_DIR/%s"
ulimit -n 10240
find -L bazel-out/ -name *.go.kzip -print0 | xargs -r0 $KYTHE_DIR/tools/kzip merge --output output.go.kzip
find -L bazel-out/ -name *.protobuf.kzip -print0 | xargs -r0 $KYTHE_DIR/tools/kzip merge --output output.protobuf.kzip

if [ -f output.go.kzip ]; then
  "$KYTHE_DIR/indexers/go_indexer" -continue output.go.kzip >> entries
fi
if [ -f output.protobuf.kzip ]; then
  "$KYTHE_DIR/indexers/proto_indexer" -index_file output.protobuf.kzip >> entries
fi
mv entries $BUILDBUDDY_ARTIFACTS_DIRECTORY/kythe_entries_for_buildbuddy
`, dirName)
	return buf
}

func KytheIndexingAction(targetRepoDefaultBranch string) *Action {
	var pushTriggerBranches []string
	if targetRepoDefaultBranch != "" {
		pushTriggerBranches = append(pushTriggerBranches, targetRepoDefaultBranch)
	}
	kytheDirName := filepath.Base(strings.TrimSuffix(kytheDownloadURL, ".tar.gz"))
	return &Action{
		Name: KytheActionName,
		Triggers: &Triggers{
			Push: &PushTrigger{Branches: pushTriggerBranches},
		},
		ContainerImage: `ubuntu-20.04`,
		ResourceRequests: ResourceRequests{
			CPU:    "16",
			Memory: "16GB",
			Disk:   "100GB",
		},
		Steps: []*rnpb.Step{
			{
				Run: checkoutKythe(kytheDirName, kytheDownloadURL),
			},
			{
				Run: buildWithKythe(kytheDirName),
			},
			{
				Run: prepareKytheOutputs(kytheDirName),
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
				BazelCommands: []string{"test //..."},
			},
		},
	}
}

// MatchesAnyTrigger returns whether the action is triggered by the event
// published to the given branch.
func MatchesAnyTrigger(action *Action, event, branch string) bool {
	// If user has manually requested action dispatch, always run it
	if event == webhook_data.EventName.ManualDispatch {
		return true
	}

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

// Returns whether the given pattern matches the given text.
// The pattern is allowed to contain a single wildcard character, "*", which
// matches anything. If there is more than one wildcard, then all wildcards
// after the first one are treated as literals.
func matchesRestrictedGlob(pattern, text string) bool {
	idx := strings.Index(pattern, "*")
	if idx == -1 {
		// No wildcard; exact match.
		return pattern == text
	}
	prefix := pattern[:idx]
	suffix := pattern[idx+1:]
	return strings.HasPrefix(text, prefix) && strings.HasSuffix(text, suffix)
}

func matchesAnyBranch(branches []string, branch string) bool {
	for _, branchPattern := range branches {
		if matchesRestrictedGlob(branchPattern, branch) {
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
