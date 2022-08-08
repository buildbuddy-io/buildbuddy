package config

import (
	"io"
	"io/ioutil"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"gopkg.in/yaml.v2"
)

// === IMPORTANT ===
// If you edit this file, also update docs/workflows-config.md
// =================

const (
	// FilePath is the path where we can expect to locate the BuildBuddyConfig
	// YAML contents, relative to the repository root.
	FilePath = "buildbuddy.yaml"
)

type BuildBuddyConfig struct {
	Actions []*Action `yaml:"actions"`
}

type Action struct {
	Name            string    `yaml:"name"`
	Triggers        *Triggers `yaml:"triggers"`
	OS              string    `yaml:"os"`
	Arch            string    `yaml:"arch"`
	GitCleanExclude []string  `yaml:"git_clean_exclude"`
	BazelCommands   []string  `yaml:"bazel_commands"`
}

type Triggers struct {
	Push        *PushTrigger        `yaml:"push"`
	PullRequest *PullRequestTrigger `yaml:"pull_request"`
}

type PushTrigger struct {
	Branches []string `yaml:"branches"`
}

type PullRequestTrigger struct {
	Branches []string `yaml:"branches"`
}

func NewConfig(r io.Reader) (*BuildBuddyConfig, error) {
	byt, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	cfg := &BuildBuddyConfig{}
	if err := yaml.Unmarshal(byt, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// GetDefault returns the default workflow config, which tests all targets
// when pushing any branch.
func GetDefault() *BuildBuddyConfig {
	return &BuildBuddyConfig{
		Actions: []*Action{
			{
				Name: "Test all targets",
				Triggers: &Triggers{
					Push: &PushTrigger{Branches: []string{"*"}},

					// TODO(bduffany): Add a PullRequest trigger to the default config
					// once we figure out a way to prevent workflows from being run twice
					// on each push to a PR branch. If this were enabled as-is, then we'd
					// get one "push" event associated with the push to the PR branch, and
					// one "pull_request" event with a "synchronized" action associated
					// with the PR.
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

func matchesAnyBranch(branches []string, branch string) bool {
	for _, b := range branches {
		if b == "*" {
			return true
		}
		if b == branch {
			return true
		}
	}
	return false
}
