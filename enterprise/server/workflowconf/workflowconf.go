package workflowconf

import (
	"io"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

const (
	// PathRelativeToRepoRoot is the path to the buildbuddy config
	// relative to the repository root.
	PathRelativeToRepoRoot = "buildbuddy.yaml"
)

type BuildBuddyConfig struct {
	Actions []*Action `yaml:"actions"`
}

type Action struct {
	Name          string    `yaml:"name"`
	Triggers      *Triggers `yaml:"triggers"`
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
