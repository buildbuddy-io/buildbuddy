package config

import (
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
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

	// KytheActionName is the name used for an action that generates Kythe annotations
	// This action is run automatically if codesearch is enabled.
	KytheActionName = "Generate Kythe Annotations"

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

const kytheDownloadURL = "https://storage.googleapis.com/buildbuddy-tools/archives/kythe-v0.0.76-buildbuddy.tar.gz"

func checkoutKythe(dirName, downloadURL string) string {
	buf := fmt.Sprintf(`
export KYTHE_DIR="$BUILDBUDDY_CI_RUNNER_ROOT_DIR/%s"
if [ ! -d "$KYTHE_DIR" ]; then
  mkdir -p "$KYTHE_DIR"
  curl -sL "%s" | tar -xz -C "$KYTHE_DIR" --strip-components 1
fi

# Bazel 8+ removed proto_lang_toolchain from native rules.
# Patch the Kythe BUILD file to load it from rules_proto.
if ! grep -q 'proto_lang_toolchain.bzl' "$KYTHE_DIR"/BUILD 2>/dev/null; then
  sed -i '1s|^|load("@rules_proto//proto:proto_lang_toolchain.bzl", "proto_lang_toolchain")\n|' "$KYTHE_DIR"/BUILD
fi

# Ensure the Kythe MODULE.bazel declares rules_proto as a dependency
# (needed when the Kythe module is registered via local_path_override).
if ! grep -q 'rules_proto' "$KYTHE_DIR"/MODULE.bazel 2>/dev/null; then
  echo -e '\nbazel_dep(name = "rules_proto", version = "7.1.0")' >> "$KYTHE_DIR"/MODULE.bazel
fi`, dirName, downloadURL)
	return buf
}

func buildWithKythe(dirName string) string {
	// TODO(jdelfino): This script doesn't pass any extra flags to Bazel, beyond those needed to
	// enable Kythe. This means the build will fail or be invalid if the normal build workflow
	// passes any important flags. While passing flags on the command line is discouraged,
	// we'll need to handle this eventually.
	bazelConfigFlags := `--config=buildbuddy_bes_backend --config=buildbuddy_bes_results_url`
	return fmt.Sprintf(`
BZL_MAJOR_VERSION=$(bazel info release | cut -d' ' -f2 | xargs | cut -d'.' -f1)

if [ $BZL_MAJOR_VERSION -lt 7 ]; then
    BZLMOD_DEFAULT=0
else
    BZLMOD_DEFAULT=1
fi

# starlark-semantics will print out enable_bzlmod if it differs from the default.
if ! bazel info starlark-semantics | grep -q "enable_bzlmod" ; then
    BZLMOD_ENABLED=$BZLMOD_DEFAULT
else
    BZLMOD_ENABLED=$(( 1 - $BZLMOD_DEFAULT ))
fi

export KYTHE_DIR="$BUILDBUDDY_CI_RUNNER_ROOT_DIR"/%s

if [ "$BZLMOD_ENABLED" -eq 1 ]; then
    # with bzlmod enabled, override_repository will not work unless the repository is already defined
	# inject_repository will work, but was added in Bazel 8, so we need to handle <8 by
	# manually adding to MODULE.bazel.
    if [ $BZL_MAJOR_VERSION -lt 8 ]; then
        echo "Adding kythe repository to MODULE.bazel"
        echo -e '\nbazel_dep(name = "kythe", version = "0.0.76")' >> MODULE.bazel
        echo "local_path_override(module_name=\"kythe\", path=\"$KYTHE_DIR\")" >> MODULE.bazel
	else
        KYTHE_ARGS="--inject_repository=kythe_release=$KYTHE_DIR"
	fi
else
    # override_repository always works if bzlmod is disabled.
	KYTHE_ARGS="--override_repository=kythe_release=$KYTHE_DIR"
fi

# These arguments make the extractors run on java generated code
KYTHE_ARGS="$KYTHE_ARGS --experimental_extra_action_top_level_only=false --experimental_extra_action_filter=^//"

# Bazel 9 defaults config_setting visibility to private, which breaks selects
if [ $BZL_MAJOR_VERSION -ge 9 ]; then
    KYTHE_ARGS="$KYTHE_ARGS --incompatible_config_setting_private_default_visibility=false"
fi

echo "Found Bazel major version: $BZL_MAJOR_VERSION, with enable_bzlmod: $BZLMOD_ENABLED"
bazel --bazelrc="$KYTHE_DIR"/extractors.bazelrc build $KYTHE_ARGS %s //...`, dirName, bazelConfigFlags)

}

func prepareKytheOutputs(dirName string) string {
	buf := fmt.Sprintf(`
export KYTHE_DIR="$BUILDBUDDY_CI_RUNNER_ROOT_DIR"/%s
ulimit -n 10240

# Note: intentionally not using xargs -P for parallel indexing, because
# parallel processes writing binary protobuf entries to a shared pipe can
# produce interleaved/corrupt output that write_tables cannot decode.
find -L bazel-out/ -name "*.go.kzip" | xargs -r -n 1 $KYTHE_DIR/indexers/go_indexer -continue | $KYTHE_DIR/tools/dedup_stream >> kythe_entries
find -L bazel-out/ -name "*.proto.kzip" | xargs -r -I {} $KYTHE_DIR/indexers/proto_indexer -index_file {} | $KYTHE_DIR/tools/dedup_stream >> kythe_entries
find -L bazel-out -name '*.java.kzip' | xargs -r -n 1 java -jar $KYTHE_DIR/indexers/java_indexer.jar | $KYTHE_DIR/tools/dedup_stream >> kythe_entries

# cxx indexing needs a cache to complete in a "reasonable" amount of time. It still takes a long time
# and produces very large indices.
# See https://groups.google.com/g/kythe/c/xKXE3S1JIRI for discussion of these args.
# TODO(jdelfino): apt update / install are slow - consider either creating a statically linked
# memcached binary, or installing it in the container image.

cxx_kzips=$(find -L bazel-out/*/extra_actions -name "*.cxx.kzip")
if [ ! -z "$cxx_kzips" ]; then
  sudo apt update && sudo apt install -y memcached
  memcached -p 11211 --listen localhost -m 512 & memcached_pid=$!
  echo "$cxx_kzips" | xargs -n 1 $KYTHE_DIR/indexers/cxx_indexer \
    --experimental_alias_template_instantiations \
	--experimental_dynamic_claim_cache="--SERVER=localhost:11211" \
	-cache="--SERVER=localhost:11211" \
	-cache_stats \
  | $KYTHE_DIR/tools/dedup_stream >> kythe_entries
  kill $memcached_pid
fi

"$KYTHE_DIR"/tools/write_tables --entries kythe_entries --out leveldb:kythe_tables
"$KYTHE_DIR"/tools/export_sstable --input leveldb:kythe_tables --output="$BUILDBUDDY_ARTIFACTS_DIRECTORY"/%s

`, dirName, accumulator.KytheOutputName)
	return buf
}

func skipIfNotBazelRepo() string {
	return `
# If this is not a Bazel repo, skip Kythe indexing.
if [ ! -f "WORKSPACE" ] && [ ! -f "WORKSPACE.bazel" ] && [ ! -f "MODULE.bazel" ]; then
  echo "No WORKSPACE, WORKSPACE.bazel, or MODULE.bazel file found. Skipping Kythe indexing."
  exit 0
fi
`
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
			CPU:    "24",   // 24 BCU
			Memory: "60GB", // 24 BCU
			Disk:   "100GB",
		},
		Steps: []*rnpb.Step{
			{
				Run: skipIfNotBazelRepo() + checkoutKythe(kytheDirName, kytheDownloadURL),
			},
			{
				Run: skipIfNotBazelRepo() + buildWithKythe(kytheDirName),
			},
			{
				Run: skipIfNotBazelRepo() + prepareKytheOutputs(kytheDirName),
			},
		},
	}
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
// published to the given branch or tag.
func MatchesAnyTrigger(action *Action, event, branch, tag string) bool {
	// If user has manually requested action dispatch, always run it
	if event == webhook_data.EventName.ManualDispatch {
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
		return matchesAnyPattern(prCfg.Branches, branch)
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
func matchesRestrictedGlob(pattern, text string) (isMatched, isNegation bool) {
	pattern, isNegation = strings.CutPrefix(pattern, "!")

	idx := strings.Index(pattern, "*")
	if idx == -1 {
		// No wildcard; exact match.
		return pattern == text, isNegation
	}
	prefix := pattern[:idx]
	suffix := pattern[idx+1:]
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
