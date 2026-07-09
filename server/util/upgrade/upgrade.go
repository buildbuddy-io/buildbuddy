// Package upgrade computes upgrade nudges: prompts telling the user that some
// component of their installation (e.g. a self-hosted cache proxy) is out of
// date, with an urgency proportional to how far behind it is.
package upgrade

import (
	"flag"
	"sync"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	uppb "github.com/buildbuddy-io/buildbuddy/proto/upgrade"
)

var (
	criticalMaxLag     = flag.String("upgrade.critical.max_lag", "", "Prompt with CRITICAL urgency when a component is further behind the newest available version than this semver-shaped diff. Empty disables this criterion.")
	criticalMinVersion = flag.String("upgrade.critical.min_version", "", "Prompt with CRITICAL urgency when a component's version is below this semver. Empty disables this criterion.")
	highMaxLag         = flag.String("upgrade.high.max_lag", "0.40.0", "Prompt with HIGH urgency when a component is further behind the newest available version than this semver-shaped diff. Empty disables this criterion.")
	highMinVersion     = flag.String("upgrade.high.min_version", "", "Prompt with HIGH urgency when a component's version is below this semver. Empty disables this criterion.")
	mediumMaxLag       = flag.String("upgrade.medium.max_lag", "0.20.0", "Prompt with MEDIUM urgency when a component is further behind the newest available version than this semver-shaped diff. Empty disables this criterion.")
	mediumMinVersion   = flag.String("upgrade.medium.min_version", "", "Prompt with MEDIUM urgency when a component's version is below this semver. Empty disables this criterion.")
	lowMaxLag          = flag.String("upgrade.low.max_lag", "0.10.0", "Prompt with LOW urgency when a component is further behind the newest available version than this semver-shaped diff. Empty disables this criterion.")
	lowMinVersion      = flag.String("upgrade.low.min_version", "", "Prompt with LOW urgency when a component's version is below this semver. Empty disables this criterion.")
)

// One "tier" of an upgrade prompt.
type prompt struct {
	urgency    uppb.Prompt_Urgency
	maxLag     *semver.Version
	minVersion *semver.Version
}

var (
	promptsOnce sync.Once
	prompts     []prompt
)

// Parses CLI flags into a list of prompts that can be checked against.
func getPrompts() []prompt {
	promptsOnce.Do(func() {
		for _, t := range []struct {
			urgency                 uppb.Prompt_Urgency
			name                    string
			maxLagFlag, minVersFlag string
		}{
			{uppb.Prompt_CRITICAL, "critical", *criticalMaxLag, *criticalMinVersion},
			{uppb.Prompt_HIGH, "high", *highMaxLag, *highMinVersion},
			{uppb.Prompt_MEDIUM, "medium", *mediumMaxLag, *mediumMinVersion},
			{uppb.Prompt_LOW, "low", *lowMaxLag, *lowMinVersion},
		} {
			prompts = append(prompts, prompt{
				urgency:    t.urgency,
				maxLag:     parseVersionFlag("upgrade."+t.name+".max_lag", t.maxLagFlag),
				minVersion: parseVersionFlag("upgrade."+t.name+".min_version", t.minVersFlag),
			})
		}
	})
	return prompts
}

func parseVersionFlag(name, value string) *semver.Version {
	if value == "" {
		return nil
	}
	v, err := semver.NewVersion(value)
	if err != nil {
		log.Warningf("Ignoring unparseable --%s flag value %q: %s", name, value, err)
		return nil
	}
	return v
}

func resetForTesting() {
	promptsOnce = sync.Once{}
	prompts = nil
}

// ForVersions returns a Prompt if any of the provided versions meet the
// desired upgrade criteria. If multiple provided versions meet it, the
// highest urgency prompt among them is returned.
func ForVersions(newest *semver.Version, versions []string) *uppb.Prompt {
	if newest == nil {
		return nil
	}
	urgency := uppb.Prompt_UNKNOWN_URGENCY
	for _, rawVersion := range versions {
		version, err := semver.NewVersion(rawVersion)
		if err != nil {
			continue
		}
		if u := getUrgency(newest, version); u > urgency {
			urgency = u
		}
	}
	if urgency == uppb.Prompt_UNKNOWN_URGENCY {
		return nil
	}
	return &uppb.Prompt{
		NewestAvailableVersion: newest.Original(),
		Urgency:                urgency,
	}
}

func getUrgency(newest, v *semver.Version) uppb.Prompt_Urgency {
	for _, t := range getPrompts() {
		if (t.minVersion != nil && v.LessThan(t.minVersion)) || lagExceeds(newest, v, t.maxLag) {
			return t.urgency
		}
	}
	return uppb.Prompt_UNKNOWN_URGENCY
}

func lagExceeds(newest, current, maxLag *semver.Version) bool {
	if maxLag == nil || !current.LessThan(newest) {
		return false
	}
	if d := int64(newest.Major()) - int64(current.Major()); d != int64(maxLag.Major()) {
		return d > int64(maxLag.Major())
	}
	if d := int64(newest.Minor()) - int64(current.Minor()); d != int64(maxLag.Minor()) {
		return d > int64(maxLag.Minor())
	}
	return int64(newest.Patch())-int64(current.Patch()) > int64(maxLag.Patch())
}
