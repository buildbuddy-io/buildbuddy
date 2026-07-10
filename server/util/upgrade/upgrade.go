// Package upgrade computes an upgrade prompt to display to the user telling
// them their installation is out of date, with configurable urgency levels.
package upgrade

import (
	"slices"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	uppb "github.com/buildbuddy-io/buildbuddy/proto/upgrade"
)

// Trigger describes when an upgrade prompt of a given urgency fires: when a
// component is further behind the newest available version than MaxLag (a
// semver-shaped diff, e.g. "0.10.0" tolerates at most 10 minor versions of
// lag), or when its version is below MinVersion. A nil criterion is ignored.
type Trigger struct {
	MaxLag     *semver.Version
	MinVersion *semver.Version
}

type level struct {
	urgency uppb.Prompt_Urgency
	trigger Trigger
}

// ParseTriggers builds the per-urgency Triggers for NewDetector from two
// urgency-to-version maps, as typically supplied via flags. Keys must be
// Prompt_Urgency enum names other than UNKNOWN_URGENCY (case-insensitive);
// values must parse as semver. Entries from both maps for the same urgency
// merge into a single Trigger.
func ParseTriggers(maxLags, minVersions map[string]string) (map[uppb.Prompt_Urgency]Trigger, error) {
	triggers := map[uppb.Prompt_Urgency]Trigger{}
	for name, lag := range maxLags {
		urgency, err := parseUrgency(name)
		if err != nil {
			return nil, err
		}
		v, err := semver.NewVersion(lag)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid max lag %q for urgency %q: %s", lag, name, err)
		}
		t := triggers[urgency]
		t.MaxLag = v
		triggers[urgency] = t
	}
	for name, minVersion := range minVersions {
		urgency, err := parseUrgency(name)
		if err != nil {
			return nil, err
		}
		v, err := semver.NewVersion(minVersion)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid min version %q for urgency %q: %s", minVersion, name, err)
		}
		t := triggers[urgency]
		t.MinVersion = v
		triggers[urgency] = t
	}
	return triggers, nil
}

func parseUrgency(name string) (uppb.Prompt_Urgency, error) {
	u, ok := uppb.Prompt_Urgency_value[strings.ToUpper(name)]
	if !ok || uppb.Prompt_Urgency(u) == uppb.Prompt_UNKNOWN_URGENCY {
		return uppb.Prompt_UNKNOWN_URGENCY, status.InvalidArgumentErrorf("invalid upgrade prompt urgency %q", name)
	}
	return uppb.Prompt_Urgency(u), nil
}

// Detector encapsulates a set of version-upgrade logic and determintes if an
// upgrade should be prompted, given the set of running versions.
type Detector struct {
	// Levels ordered most-urgent first; the first trigger a version matches
	// determines its urgency.
	levels []level
}

func NewDetector(triggers map[uppb.Prompt_Urgency]Trigger) *Detector {
	d := &Detector{}
	for urgency, trigger := range triggers {
		d.levels = append(d.levels, level{urgency: urgency, trigger: trigger})
	}
	slices.SortFunc(d.levels, func(a, b level) int {
		return int(b.urgency) - int(a.urgency)
	})
	return d
}

// Detect returns a Prompt carrying newest's version and the given message if
// any of the provided versions meet one of the detector's triggers, and nil
// otherwise. If multiple versions match, the prompt reflects the highest
// urgency among them. Versions that don't parse as semver (e.g. "unknown"
// from dev builds) are skipped.
func (d *Detector) Detect(newest *semver.Version, versions []string, message string) *uppb.Prompt {
	if newest == nil {
		return nil
	}
	urgency := uppb.Prompt_UNKNOWN_URGENCY
	for _, rawVersion := range versions {
		version, err := semver.NewVersion(rawVersion)
		if err != nil {
			continue
		}
		if u := d.urgency(newest, version); u > urgency {
			urgency = u
		}
	}
	if urgency == uppb.Prompt_UNKNOWN_URGENCY {
		return nil
	}
	return &uppb.Prompt{
		NewestAvailableVersion: newest.Original(),
		Urgency:                urgency,
		Message:                message,
	}
}

func (d *Detector) urgency(newest, v *semver.Version) uppb.Prompt_Urgency {
	for _, l := range d.levels {
		if (l.trigger.MinVersion != nil && v.LessThan(l.trigger.MinVersion)) || lagExceeds(newest, v, l.trigger.MaxLag) {
			return l.urgency
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
