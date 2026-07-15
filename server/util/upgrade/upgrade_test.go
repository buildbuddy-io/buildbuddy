package upgrade

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	uppb "github.com/buildbuddy-io/buildbuddy/proto/upgrade"
)

func testDetector() *Detector {
	return NewDetector(map[uppb.Prompt_Urgency]Trigger{
		uppb.Prompt_LOW:    {MaxLag: semver.MustParse("0.10.0")},
		uppb.Prompt_MEDIUM: {MaxLag: semver.MustParse("0.20.0")},
		uppb.Prompt_HIGH:   {MaxLag: semver.MustParse("0.40.0")},
	})
}

func TestUrgency_MaxLag(t *testing.T) {
	d := testDetector()
	for _, tc := range []struct {
		newest, v string
		want      uppb.Prompt_Urgency
	}{
		{"v2.153.0", "v2.153.0", uppb.Prompt_UNKNOWN_URGENCY},
		// Exactly at the low level's "0.10.0" allowance: no prompt.
		{"v2.153.0", "v2.143.0", uppb.Prompt_UNKNOWN_URGENCY},
		// One patch version beyond the "0.10.0" allowance: prompt.
		{"v2.153.1", "v2.143.0", uppb.Prompt_LOW},
		{"v2.153.0", "v2.142.0", uppb.Prompt_LOW},
		// Exactly at the medium level's "0.20.0" allowance: still LOW.
		{"v2.153.0", "v2.133.0", uppb.Prompt_LOW},
		{"v2.153.0", "v2.132.0", uppb.Prompt_MEDIUM},
		{"v2.153.0", "v2.113.0", uppb.Prompt_MEDIUM},
		{"v2.153.0", "v2.112.0", uppb.Prompt_HIGH},
		// Any major-version lag exceeds all the "0.x.0" allowances, and this
		// detector has no critical level, so HIGH is its ceiling.
		{"v3.0.0", "v2.153.0", uppb.Prompt_HIGH},
		// A component newer than "newest" (stale data) never prompts.
		{"v2.153.0", "v2.154.0", uppb.Prompt_UNKNOWN_URGENCY},
		{"v2.153.0", "v3.0.0", uppb.Prompt_UNKNOWN_URGENCY},
	} {
		newest := semver.MustParse(tc.newest)
		v := semver.MustParse(tc.v)
		assert.Equal(t, tc.want, d.urgency(newest, v), "newest=%s v=%s", tc.newest, tc.v)
	}
}

func TestUrgency_MajorVersionAllowances(t *testing.T) {
	d := NewDetector(map[uppb.Prompt_Urgency]Trigger{
		uppb.Prompt_LOW:      {MaxLag: semver.MustParse("0.2.0")},
		uppb.Prompt_MEDIUM:   {MaxLag: semver.MustParse("0.6.0")},
		uppb.Prompt_HIGH:     {MaxLag: semver.MustParse("1.0.0")},
		uppb.Prompt_CRITICAL: {MaxLag: semver.MustParse("2.0.0")},
	})

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
		assert.Equal(t, tc.want, d.urgency(newest, semver.MustParse(tc.v)), "v=%s", tc.v)
	}
}

func TestUrgency_MinVersion(t *testing.T) {
	d := NewDetector(map[uppb.Prompt_Urgency]Trigger{
		uppb.Prompt_LOW:      {MaxLag: semver.MustParse("0.10.0"), MinVersion: semver.MustParse("2.153.0")},
		uppb.Prompt_MEDIUM:   {MaxLag: semver.MustParse("0.20.0")},
		uppb.Prompt_HIGH:     {MaxLag: semver.MustParse("0.40.0")},
		uppb.Prompt_CRITICAL: {MinVersion: semver.MustParse("2.150.0")},
	})

	newest := semver.MustParse("v2.153.0")
	// Below the critical minimum: CRITICAL even though the lag alone
	// wouldn't prompt at all.
	assert.Equal(t, uppb.Prompt_CRITICAL, d.urgency(newest, semver.MustParse("v2.149.5")))
	// Below the low minimum but within every lag allowance.
	assert.Equal(t, uppb.Prompt_LOW, d.urgency(newest, semver.MustParse("v2.152.9")))
	// At the low minimum: no prompt.
	assert.Equal(t, uppb.Prompt_UNKNOWN_URGENCY, d.urgency(newest, semver.MustParse("v2.153.0")))
}

func TestUrgency_NoTriggers(t *testing.T) {
	// A detector with no triggers (or triggers with no criteria) never
	// prompts.
	for _, d := range []*Detector{
		NewDetector(nil),
		NewDetector(map[uppb.Prompt_Urgency]Trigger{
			uppb.Prompt_LOW:      {},
			uppb.Prompt_CRITICAL: {},
		}),
	} {
		newest := semver.MustParse("v3.0.0")
		assert.Equal(t, uppb.Prompt_UNKNOWN_URGENCY, d.urgency(newest, semver.MustParse("v1.0.0")))
	}
}

func TestDetect(t *testing.T) {
	d := testDetector()
	message := "please upgrade"
	newest := semver.MustParse("v2.153.0")

	// The most-outdated version sets the urgency; unparseable versions are
	// skipped.
	prompt := d.Detect(newest, []string{"v2.153.0", "unknown", "v2.132.0"}, message)
	require.NotNil(t, prompt)
	assert.Equal(t, uppb.Prompt_MEDIUM, prompt.GetUrgency())
	assert.Equal(t, message, prompt.GetMessage())

	// Everything within the allowances: no prompt.
	assert.Nil(t, d.Detect(newest, []string{"v2.153.0", "v2.152.0"}, message))

	// Only unparseable versions: no prompt.
	assert.Nil(t, d.Detect(newest, []string{"unknown"}, message))

	// No newest version known: no prompt.
	assert.Nil(t, d.Detect(nil, []string{"v2.100.0"}, message))
}
