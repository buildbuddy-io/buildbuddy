package parser

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
)

func init() {
	log.Configure([]string{"--verbose=1"})
}

func TestParseBazelrc_Basic(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE": "",
		".bazelrc": `

# COMMENT
#ANOTHER COMMENT
#

# continuations are allowed \
--this_is_not_a_flag_since_it_is_part_of_the_previous_line

--common_global_flag_1          # trailing comments are allowed
common --common_global_flag_2
common:foo --config_foo_global_flag
common:bar --config_bar_global_flag

build --build_flag_1
build:foo --build_config_foo_flag

# Should be able to refer to the "forward_ref" config even though
# it comes later on in the file
build:foo --config=forward_ref

build:foo --build_config_foo_multi_1 --build_config_foo_multi_2

build:forward_ref --build_config_forward_ref_flag

build:bar --build_config_bar_flag
`,
	})

	for _, tc := range []struct {
		args                 []string
		expectedExpandedArgs []string
	}{
		{
			[]string{"query"},
			[]string{
				"query",
				"--common_global_flag_1",
				"--common_global_flag_2",
			},
		},
		{
			[]string{"build"},
			[]string{
				"build",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
			},
		},
		{
			[]string{"build", "--explicit_flag"},
			[]string{
				"build",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
				"--explicit_flag",
			},
		},
		{
			[]string{"build", "--config=foo"},
			[]string{
				"build",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
				"--config_foo_global_flag",
				"--build_config_foo_flag",
				"--build_config_forward_ref_flag",
				"--build_config_foo_multi_1",
				"--build_config_foo_multi_2",
			},
		},
		{
			[]string{"build", "--config=foo", "--config", "bar"},
			[]string{
				"build",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
				"--config_foo_global_flag",
				"--build_config_foo_flag",
				"--build_config_forward_ref_flag",
				"--build_config_foo_multi_1",
				"--build_config_foo_multi_2",
				"--config_bar_global_flag",
				"--build_config_bar_flag",
			},
		},
	} {
		expandedArgs := expandConfigs(ws, tc.args)

		assert.Equal(t, tc.expectedExpandedArgs, expandedArgs)
	}
}
