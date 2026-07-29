package main

import (
	"slices"
	"strings"
)

// A compact execution log records every spawn a build ran, labelled with the
// target it belongs to and the mnemonic of the action that produced it. The
// merge keeps only those labels - which target ran which kind of action, in
// which builds - because that is cheap enough to do for every log.
//
// What a step consumed and produced is not. Input sets are most of a log's
// bytes, and flattening them for every target of every log would dwarf the tree
// many times over. Those are read back off a single log, for a single target,
// when the target is actually asked about; see spawnGraph.filesFor.

// targetSteps is one target as the merge saw it: the logs it ran in, and the
// steps it ran there.
type targetSteps struct {
	label string
	// logs are the indexes of the logs the target appeared in, in merge order.
	logs      []int
	mnemonics map[string]*mnemonicSteps
}

// mnemonicSteps is one kind of action within a target, and the logs that ran
// it. A target usually runs a few: compile, link, test.
type mnemonicSteps struct {
	name string
	logs []int
	// platforms is how many builds ran the step on each os/arch, for the builds
	// whose spawns said. A build that ran it on more than one counts under each.
	platforms map[string]*platformCount
}

// platformCount is how many builds ran a step on one os/arch, and the last log
// that did, so that a build's many spawns count once between them.
type platformCount struct {
	builds  int
	lastLog int
}

// addStep records that a log ran a step of a target.
func (t *tree) addStep(logIdx int, step spawnStep) {
	ts := t.targets[step.target]
	if ts == nil {
		ts = &targetSteps{label: step.target, mnemonics: map[string]*mnemonicSteps{}}
		t.targets[step.target] = ts
	}
	ts.logs = appendLog(ts.logs, logIdx)
	ms := ts.mnemonics[step.mnemonic]
	if ms == nil {
		ms = &mnemonicSteps{name: step.mnemonic, platforms: map[string]*platformCount{}}
		ts.mnemonics[step.mnemonic] = ms
	}
	ms.logs = appendLog(ms.logs, logIdx)
	if step.platform == "" {
		return
	}
	p := ms.platforms[step.platform]
	if p == nil {
		p = &platformCount{lastLog: -1}
		ms.platforms[step.platform] = p
	}
	if p.lastLog != logIdx {
		p.builds++
		p.lastLog = logIdx
	}
}

// platformTally is one os/arch and how many builds ran a step there.
type platformTally struct {
	platform string
	builds   int
}

// sortedPlatforms returns where the step ran, the most-used first.
func (ms *mnemonicSteps) sortedPlatforms() []platformTally {
	tallies := make([]platformTally, 0, len(ms.platforms))
	for platform, count := range ms.platforms {
		tallies = append(tallies, platformTally{platform: platform, builds: count.builds})
	}
	slices.SortFunc(tallies, func(a, b platformTally) int {
		if n := b.builds - a.builds; n != 0 {
			return n
		}
		return strings.Compare(a.platform, b.platform)
	})
	return tallies
}

// appendLog records a log against a step, ignoring the repeats: a target runs
// many spawns per build. Logs are merged one at a time and in order, so a log
// that's already been recorded can only be the last one.
func appendLog(logs []int, logIdx int) []int {
	if len(logs) > 0 && logs[len(logs)-1] == logIdx {
		return logs
	}
	return append(logs, logIdx)
}

// sortedTargets returns every target the logs ran, the ones seen in the most
// builds first.
func (t *tree) sortedTargets() []*targetSteps {
	targets := make([]*targetSteps, 0, len(t.targets))
	for _, ts := range t.targets {
		targets = append(targets, ts)
	}
	slices.SortFunc(targets, func(a, b *targetSteps) int {
		if n := len(b.logs) - len(a.logs); n != 0 {
			return n
		}
		return strings.Compare(a.label, b.label)
	})
	return targets
}

// sortedMnemonics returns the target's steps, the ones seen in the most builds
// first.
func (ts *targetSteps) sortedMnemonics() []*mnemonicSteps {
	mnemonics := make([]*mnemonicSteps, 0, len(ts.mnemonics))
	for _, ms := range ts.mnemonics {
		mnemonics = append(mnemonics, ms)
	}
	slices.SortFunc(mnemonics, func(a, b *mnemonicSteps) int {
		if n := len(b.logs) - len(a.logs); n != 0 {
			return n
		}
		return strings.Compare(a.name, b.name)
	})
	return mnemonics
}

// sortLogsByTime orders log indexes newest build first. Logs fetched from a
// server carry the invocation's timestamp; logs read from files don't, and sort
// after them in the order they were merged.
func (t *tree) sortLogsByTime(logs []int) []int {
	sorted := slices.Clone(logs)
	slices.SortStableFunc(sorted, func(a, b int) int {
		aAt, bAt := t.log(a).updatedAtUsec, t.log(b).updatedAtUsec
		switch {
		case aAt == bAt:
			return a - b
		case aAt == 0:
			return 1
		case bAt == 0:
			return -1
		}
		// Newest first.
		return int(min(max(bAt-aAt, -1), 1))
	})
	return sorted
}
