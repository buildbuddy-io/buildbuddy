package upgrade

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	uppb "github.com/buildbuddy-io/buildbuddy/proto/upgrade"
)

// setFlag changes an upgrade flag and clears the parsed-flag cache, both for
// this test and (via cleanup) for whichever test runs next.
func setFlag(t *testing.T, name, value string) {
	flags.Set(t, name, value)
	resetForTesting()
	t.Cleanup(resetForTesting)
}

func TestUrgency_MaxLagDefaults(t *testing.T) {
	for _, tc := range []struct {
		newest, v string
		want      uppb.Prompt_Urgency
	}{
		{"v2.153.0", "v2.153.0", uppb.Prompt_UNKNOWN_URGENCY},
		// Exactly at the low tier's "0.10.0" allowance: no prompt.
		{"v2.153.0", "v2.143.0", uppb.Prompt_UNKNOWN_URGENCY},
		// One patch version beyond the "0.10.0" allowance: prompt.
		{"v2.153.1", "v2.143.0", uppb.Prompt_LOW},
		{"v2.153.0", "v2.142.0", uppb.Prompt_LOW},
		// Exactly at the medium tier's "0.20.0" allowance: still LOW.
		{"v2.153.0", "v2.133.0", uppb.Prompt_LOW},
		{"v2.153.0", "v2.132.0", uppb.Prompt_MEDIUM},
		{"v2.153.0", "v2.113.0", uppb.Prompt_MEDIUM},
		{"v2.153.0", "v2.112.0", uppb.Prompt_HIGH},
		// Any major-version lag exceeds all the default "0.x.0" allowances,
		// and the critical tier is disabled by default, so HIGH is the
		// default ceiling.
		{"v3.0.0", "v2.153.0", uppb.Prompt_HIGH},
		// A component newer than "newest" (stale data) never prompts.
		{"v2.153.0", "v2.154.0", uppb.Prompt_UNKNOWN_URGENCY},
		{"v2.153.0", "v3.0.0", uppb.Prompt_UNKNOWN_URGENCY},
	} {
		newest := semver.MustParse(tc.newest)
		v := semver.MustParse(tc.v)
		assert.Equal(t, tc.want, getUrgency(newest, v), "newest=%s v=%s", tc.newest, tc.v)
	}
}

func TestUrgency_MaxLagConfigured(t *testing.T) {
	setFlag(t, "upgrade.low.max_lag", "0.2.0")
	setFlag(t, "upgrade.medium.max_lag", "0.6.0")
	setFlag(t, "upgrade.high.max_lag", "1.0.0")
	setFlag(t, "upgrade.critical.max_lag", "2.0.0")

	newest := semver.MustParse("v3.10.0")
	for _, tc := range []struct {
		v    string
		want uppb.Prompt_Urgency
	}{
		{"v3.8.0", uppb.Prompt_UNKNOWN_URGENCY},
		{"v3.7.0", uppb.Prompt_LOW},
		{"v3.3.0", uppb.Prompt_MEDIUM},
		// With a "1.0.0" high allowance, minor-version lag alone never
		// reaches HIGH; being more than a full major version behind does.
		{"v3.0.0", uppb.Prompt_MEDIUM},
		{"v2.10.0", uppb.Prompt_MEDIUM},
		{"v2.9.0", uppb.Prompt_HIGH},
		{"v1.0.0", uppb.Prompt_CRITICAL},
	} {
		assert.Equal(t, tc.want, getUrgency(newest, semver.MustParse(tc.v)), "v=%s", tc.v)
	}
}

func TestUrgency_MinVersion(t *testing.T) {
	setFlag(t, "upgrade.critical.min_version", "2.150.0")
	setFlag(t, "upgrade.low.min_version", "2.153.0")

	newest := semver.MustParse("v2.153.0")
	// Below the critical minimum: CRITICAL even though the lag alone
	// wouldn't prompt at all.
	assert.Equal(t, uppb.Prompt_CRITICAL, getUrgency(newest, semver.MustParse("v2.149.5")))
	// Below the low minimum but within every lag allowance.
	assert.Equal(t, uppb.Prompt_LOW, getUrgency(newest, semver.MustParse("v2.152.9")))
	// At the low minimum: no prompt.
	assert.Equal(t, uppb.Prompt_UNKNOWN_URGENCY, getUrgency(newest, semver.MustParse("v2.153.0")))
}

func TestUrgency_DisabledTiers(t *testing.T) {
	// With every criterion disabled, nothing nudges.
	for _, name := range []string{
		"upgrade.low.max_lag",
		"upgrade.medium.max_lag",
		"upgrade.high.max_lag",
		"upgrade.critical.max_lag",
	} {
		setFlag(t, name, "")
	}
	newest := semver.MustParse("v3.0.0")
	assert.Equal(t, uppb.Prompt_UNKNOWN_URGENCY, getUrgency(newest, semver.MustParse("v1.0.0")))
}

func TestForVersions(t *testing.T) {
	newest := semver.MustParse("v2.153.0")

	// The most-outdated version sets the urgency; unparseable versions are
	// skipped.
	n := ForVersions(newest, []string{"v2.153.0", "unknown", "v2.132.0"})
	require.NotNil(t, n)
	assert.Equal(t, "v2.153.0", n.GetNewestAvailableVersion())
	assert.Equal(t, uppb.Prompt_MEDIUM, n.GetUrgency())

	// Everything within the allowances: no prompt.
	assert.Nil(t, ForVersions(newest, []string{"v2.153.0", "v2.152.0"}))

	// Only unparseable versions: no prompt.
	assert.Nil(t, ForVersions(newest, []string{"unknown"}))

	// No newest version known: no prompt.
	assert.Nil(t, ForVersions(nil, []string{"v2.100.0"}))
}
