package remotebazel

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
)

func TestParseRemoteCliFlags(t *testing.T) {
	type testCase struct {
		name              string
		inputArgs         []string
		expectedOutput    []string
		expectedFlagValue map[string]string
		expectedError     bool
	}

	testCases := []testCase{
		{
			name: "one remote cli flag",
			inputArgs: []string{
				"--remote_runner=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
			},
		},
		{
			name: "one remote cli flag - space between val",
			inputArgs: []string{
				"--remote_runner",
				"val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
			},
		},
		{
			name: "multiple remote cli flags",
			inputArgs: []string{
				"--remote_runner=val",
				"--os=val2",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name: "repeated remote cli flags",
			inputArgs: []string{
				"--env=key=val",
				"--remote_runner=val",
				"--env=key2=val2",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"env":           "key=val,key2=val2",
			},
		},
		{
			name: "no flags",
			inputArgs: []string{
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
		},
		{
			name: "startup flags, but no cli flags",
			inputArgs: []string{
				"--output_base=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"build",
				"//...",
			},
		},
		{
			name: "startup flags, but no cli flags - space between value",
			inputArgs: []string{
				"--output_base",
				"val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base",
				"val",
				"build",
				"//...",
			},
		},
		{
			name: "mix of startup flags and cli flags - starting with cli flag",
			inputArgs: []string{
				"--os",
				"val2",
				"--output_base=val",
				"--remote_runner=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name: "mix of startup flags and cli flags - starting with startup flag",
			inputArgs: []string{
				"--output_base=val",
				"--os",
				"val2",
				"--remote_runner=val",
				"--system_rc",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"--system_rc",
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name:              "empty",
			inputArgs:         []string{},
			expectedOutput:    []string{},
			expectedFlagValue: map[string]string{},
			expectedError:     true,
		},
		{
			name: "flags after the bazel command shouldn't be affected",
			inputArgs: []string{
				"--os",
				"val2",
				"build",
				"//...",
				"--os=untouched",
			},
			expectedOutput: []string{
				"build",
				"//...",
				"--os=untouched",
			},
			expectedFlagValue: map[string]string{
				"os": "val2",
			},
		},
	}
	for _, tc := range testCases {
		actualOutput, err := parseRemoteCliFlags(tc.inputArgs)
		if tc.expectedError {
			require.Error(t, err, tc.name)
		} else {
			require.NoError(t, err, tc.name)
			require.Equal(t, tc.expectedOutput, actualOutput, tc.name)
		}

		for flag, expectedVal := range tc.expectedFlagValue {
			actualVal := RemoteFlagset.Lookup(flag).Value
			require.Equal(t, expectedVal, actualVal.String(), tc.name)
		}
	}
}

func TestGitConfig_BranchAndSha(t *testing.T) {
	// Setup the "remote" repo
	remoteRepoPath, remoteMasterHeadCommit := testgit.MakeTempRepo(t, map[string]string{"hello.txt": "exit 0"})

	// Create a remote branch
	testshell.Run(t, remoteRepoPath, "git checkout -B remote_b")
	remoteBranchHeadCommit := testgit.CommitFiles(t, remoteRepoPath, map[string]string{"new_file.txt": "exit 0"})
	testshell.Run(t, remoteRepoPath, "git checkout master")

	type testCase struct {
		name string

		localBranchExistsRemotely bool
		localCommitExistsRemotely bool

		expectedBranch  string
		expectedCommit  string
		expectedPatches []string
	}

	testCases := []testCase{
		{
			name:                      "Local branch and commit exist remotely",
			localBranchExistsRemotely: true,
			localCommitExistsRemotely: true,
			expectedBranch:            "remote_b",
			expectedCommit:            remoteBranchHeadCommit,
			expectedPatches:           []string{},
		},
		{
			name:                      "Local branch does not exist remotely",
			localBranchExistsRemotely: false,
			localCommitExistsRemotely: false,
			expectedBranch:            "master",
			expectedCommit:            remoteMasterHeadCommit,
			expectedPatches:           []string{"local_file.txt"},
		},
		{
			name:                      "Local commit does not exist remotely",
			localBranchExistsRemotely: true,
			localCommitExistsRemotely: false,
			expectedBranch:            "remote_b",
			expectedCommit:            remoteBranchHeadCommit,
			expectedPatches:           []string{"local_file.txt"},
		},
	}

	for _, tc := range testCases {
		// Setup a "local" repo
		localRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
		err := os.Chdir(localRepoPath)
		require.NoError(t, err, tc.name)

		if tc.localBranchExistsRemotely {
			testshell.Run(t, localRepoPath, "git checkout remote_b")
		} else {
			testshell.Run(t, localRepoPath, "git checkout -B local_only")
		}
		if !tc.localCommitExistsRemotely {
			testgit.CommitFiles(t, localRepoPath, map[string]string{"local_file.txt": "exit 0"})
		}

		config, err := Config()
		require.NoError(t, err, tc.name)

		require.Equal(t, tc.expectedBranch, config.Ref, tc.name)
		require.Equal(t, tc.expectedCommit, config.CommitSHA, tc.name)
		require.Equal(t, len(tc.expectedPatches), len(config.Patches))
		if len(tc.expectedPatches) > 0 {
			require.Contains(t, string(config.Patches[0]), tc.expectedPatches[0])
		}
	}
}

func TestGitConfig_FetchURL(t *testing.T) {
	// Setup the "remote" repo
	remoteRepoPath, _ := testgit.MakeTempRepo(t, map[string]string{"hello.txt": "exit 0"})
	remoteUrl := "file://" + remoteRepoPath

	testCases := []struct {
		name            string
		expectedURL     string
		multipleRemotes bool
		isRemoteCached  bool
	}{
		{
			name:        "One remote is configured",
			expectedURL: remoteUrl,
		},
		{
			name:            "Selected remote is cached",
			multipleRemotes: true,
			isRemoteCached:  true,
			expectedURL:     remoteUrl,
		},
	}

	for _, tc := range testCases {
		// Setup a "local" repo
		localRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
		err := os.Chdir(localRepoPath)
		require.NoError(t, err, tc.name)

		if tc.multipleRemotes {
			testshell.Run(t, localRepoPath, "git remote add extra "+remoteUrl)
		}
		if tc.isRemoteCached {
			testshell.Run(t, localRepoPath, fmt.Sprintf("git config --replace-all %s.%s extra", gitConfigSection, gitConfigRemoteBazelRemote))
		}

		config, err := Config()
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.expectedURL, config.URL)
	}
}

func TestGeneratingPatches(t *testing.T) {
	// Setup the "remote" repo
	remoteRepoPath, _ := testgit.MakeTempRepo(t, map[string]string{
		"hello.txt": "echo HI",
		"b.bin":     "",
	})

	// Setup a "local" repo
	localRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
	// Remote bazel runs commands in the working directory, so make sure it
	// is set correctly
	err := os.Chdir(localRepoPath)
	require.NoError(t, err)

	testshell.Run(t, localRepoPath, `
		# Generate a diff on a pre-existing file
		echo "echo HELLO" > hello.txt

		# Generate a diff for a new untracked file
		echo "echo BYE" > bye.txt

		# Generate a binary diff on a pre-existing file
		echo -ne '\x00\x01\x02\x03\x04' > b.bin

		# Generate a binary diff on an untracked file
		echo -ne '\x00\x01\x02\x03\x04' > b2.bin
`)

	config, err := Config()
	require.NoError(t, err)

	require.Equal(t, 4, len(config.Patches))
	for _, patchBytes := range config.Patches {
		p := string(patchBytes)
		if strings.Contains(p, "hello.txt") {
			require.Contains(t, p, "HELLO")
		} else if strings.Contains(p, "bye.txt") {
			require.Contains(t, p, "BYE")
		} else if strings.Contains(p, "b.bin") {
			require.Contains(t, p, "GIT binary patch")
		} else if strings.Contains(p, "b2.bin") {
			require.Contains(t, p, "GIT binary patch")
		} else {
			require.FailNowf(t, "unexpected patch %s", p)
		}
	}
}
