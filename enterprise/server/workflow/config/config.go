package config

import (
	"fmt"
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
	Name          string    `yaml:"name"`
	Triggers      *Triggers `yaml:"triggers"`
	OS            string    `yaml:"os"`
	Arch          string    `yaml:"arch"`
	BazelCommands []string  `yaml:"bazel_commands"`
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
// when pushing to the given target branch, sending build events to BuildBuddy.
func GetDefault(targetBranch, besBackend, besResultsURL, apiKey string) *BuildBuddyConfig {
	return &BuildBuddyConfig{
		Actions: []*Action{
			{
				Name: "Test all targets",
				Triggers: &Triggers{
					Push: &PushTrigger{Branches: []string{targetBranch}},
				},
				BazelCommands: []string{
					fmt.Sprintf(
						"test //... "+
							"--build_metadata=ROLE=CI "+
							"--bes_backend=%s --bes_results_url=%s "+
							"--remote_header=x-buildbuddy-api-key=%s",
						besBackend, besResultsURL,
						apiKey,
					),
				},
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
		if b == branch {
			return true
		}
	}
	return false
}
