package config

import (
	"io"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

// === IMPORTANT ===
//
// NOTE: Lines starting with "///" are used to generate markdown documentation
// for the YAML schema under `docs/workflows-schema.md`.
//
// Re-generate those docs whenever you change this file by running:
//
// python3 enterprise/server/workflow/config/generate_docs.py
//
// Then see website/README.md for instructions on how to view the doc site,
// and load up the "Workflows config" tab and make sure everything looks good.
//
// =================

/// The top-level BuildBuddy workflow config, which specifies bazel commands
/// that can be run on a repo, as well as the events that trigger those commands.
type BuildBuddyConfig struct {
	/// List of actions that can be triggered by BuildBuddy.
	///
	/// If multiple actions are matched for a given event, the actions are run in
	/// order. If an action fails, subsequent actions will still be executed.
	Actions []*Action `yaml:"actions"`
}

/// A named group of Bazel commands that run when triggered.
type Action struct {
	/// A name unique to this config, which shows up in reported GitHub statuses.
	Name string `yaml:"name"`
	/// The triggers for this action.
	Triggers *Triggers `yaml:"triggers"`
	/// Bazel commands to be run in order. If a command fails, subsequent ones
	/// are not run, and the action fails. Otherwise, the action succeeds.
	BazelCommands []string `yaml:"bazel_commands"`
}

/// Defines whether an action should run when a branch is pushed to the repo.
type Triggers struct {
	/// Configuration for push events associated with the repo.
	///
	/// This is mostly useful for reporting commit statuses that show up on the
	/// home page of the repo.
	Push *PushTrigger `yaml:"push"`
	/// Configuration for pull request events associated with the repo.
	///
	/// This is required if you want to use BuildBuddy to report the status of
	/// this action on pull requests, and optionally prevent pull requests from
	/// being merged if the action fails.
	PullRequest *PullRequestTrigger `yaml:"pull_request"`
}

/// Defines whether an action should execute when a branch is pushed.
type PushTrigger struct {
	/// The branches that, when pushed to, will trigger the action.
	Branches []string `yaml:"branches"`
}

/// Defines whether an action should execute when a pull request (PR) branch is
/// pushed.
type PullRequestTrigger struct {
	/// The _target_ branches of a pull request that must match push events
	///
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
