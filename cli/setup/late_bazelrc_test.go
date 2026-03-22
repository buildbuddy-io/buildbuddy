package setup

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Configure("--verbose=1")
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

func TestImportantLateBazelrcChanges_DetectsImportantFlags(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	lateBazelrc := testfs.WriteFile(t, dir, "late.bazelrc", "build --bes_backend=grpcs://remote.buildbuddy.dev")

	lateFiles, changedFlags, err := importantLateBazelrcChanges(
		[]string{"--nosystem_rc", "--noworkspace_rc", "--nohome_rc", "build"},
		[]string{"--nosystem_rc", "--noworkspace_rc", "--nohome_rc", "--bazelrc=" + lateBazelrc, "build"},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{lateBazelrc}, lateFiles)
	assert.Equal(t, []string{"bes_backend"}, changedFlags)
}

func TestImportantLateBazelrcChanges_IgnoresUnimportantFlags(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	lateBazelrc := testfs.WriteFile(t, dir, "late.bazelrc", "build --color=yes")

	lateFiles, changedFlags, err := importantLateBazelrcChanges(
		[]string{"--nosystem_rc", "--noworkspace_rc", "--nohome_rc", "build"},
		[]string{"--nosystem_rc", "--noworkspace_rc", "--nohome_rc", "--bazelrc=" + lateBazelrc, "build"},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{lateBazelrc}, lateFiles)
	assert.Empty(t, changedFlags)
}

func TestImportantLateBazelrcChanges_RespectsExplicitBazelrcBarrier(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	lateBazelrc := testfs.WriteFile(t, dir, "late.bazelrc", "build --bes_backend=grpcs://remote.buildbuddy.dev")

	lateFiles, changedFlags, err := importantLateBazelrcChanges(
		[]string{"--nosystem_rc", "--noworkspace_rc", "--nohome_rc", "--bazelrc=/dev/null", "build"},
		[]string{"--nosystem_rc", "--noworkspace_rc", "--nohome_rc", "--bazelrc=/dev/null", "--bazelrc=" + lateBazelrc, "build"},
	)
	require.NoError(t, err)
	assert.Empty(t, lateFiles)
	assert.Empty(t, changedFlags)
}
