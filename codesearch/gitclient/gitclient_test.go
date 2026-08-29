package gitclient

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
)

type fakeGitClient struct {
	commands map[string]string
	files    map[string][]byte
	t        *testing.T
}

func incrementalLogCommand(firstSHA, lastSHA string) string {
	return fmt.Sprintf("log --raw --first-parent --format=%%H --reverse %s..%s", firstSHA, lastSHA)
}

func (f *fakeGitClient) ExecuteCommand(args ...string) (string, error) {
	fullCmd := strings.Join(args, " ")
	if output, ok := f.commands[fullCmd]; ok {
		return output, nil
	}
	require.FailNow(f.t, "command not found", "cmd: %s", fullCmd)
	return "", fmt.Errorf("command not found: %s", fullCmd)
}

func (f *fakeGitClient) LoadFileContents(fileToLoad string) ([]byte, error) {
	if contents, ok := f.files[fileToLoad]; ok {
		return contents, nil
	}
	require.FailNow(f.t, "file not found", "file: %s", fileToLoad)
	return nil, fmt.Errorf("file not found: %s", fileToLoad)
}

func TestComputeIncrementalUpdate_OneCommit(t *testing.T) {
	firstSHA := "abc123"
	lastSHA := "def456"

	fakeClient := &fakeGitClient{
		t: t,
		commands: map[string]string{
			incrementalLogCommand(firstSHA, lastSHA): `
def456

:100644 100644 bcd1234 0123456 M	file0
:100644 100644 abcd123 1234567 C68	file0	file2
:100644 100644 abcd123 1234567 R86	file1	file3
:000000 100644 0000000 1234567 A	file4
:100644 000000 1234567 0000000 D	file5
:000000 000000 0000000 0000000 U	file6
`,
		},
		files: map[string][]byte{
			"file0": []byte("file0 content"),
			// file1 renamed to file2
			"file2": []byte("file2 content"),
			"file3": []byte("file3 content"),
			"file4": []byte("file4 content"),
			// file5 deleted
			// file6 unmerged, should be ignored
		},
	}

	result, err := ComputeIncrementalUpdate(fakeClient, firstSHA, lastSHA)
	require.NoError(t, err)

	assert.Equal(t, &inpb.IncrementalUpdate{
		Commits: []*inpb.Commit{
			{
				Sha:       "def456",
				ParentSha: "abc123",
				AddsAndUpdates: []*inpb.File{
					{Filepath: "file0", Content: []byte("file0 content")},
					{Filepath: "file2", Content: []byte("file2 content")},
					{Filepath: "file3", Content: []byte("file3 content")},
					{Filepath: "file4", Content: []byte("file4 content")},
				},
				DeleteFilepaths: []string{"file1", "file5"},
			},
		},
	}, result)
}

func TestComputeIncrementalUpdate_MultipleCommits(t *testing.T) {
	sha1 := "aaa123"
	sha2 := "bbb456"
	sha3 := "ccc789"
	sha4 := "ddd012"

	fakeClient := &fakeGitClient{
		t: t,
		commands: map[string]string{
			incrementalLogCommand(sha1, sha4): `
bbb456

:100644 100644 bcd1234 0123456 M	file0
ccc789

:000000 100644 0000000 1234567 A	file1
ddd012

:100644 100644 abcd123 1234567 R86	file1	file2
`,
		},
		files: map[string][]byte{
			"file0": []byte("file0 content"),
			"file1": []byte("file1 content"),
			"file2": []byte("file2 content"),
		},
	}

	result, err := ComputeIncrementalUpdate(fakeClient, sha1, sha4)
	require.NoError(t, err)

	assert.Equal(t, &inpb.IncrementalUpdate{
		Commits: []*inpb.Commit{
			{
				Sha:       sha2,
				ParentSha: sha1,
				AddsAndUpdates: []*inpb.File{
					{Filepath: "file0", Content: []byte("file0 content")},
				},
			},
			{
				Sha:       sha3,
				ParentSha: sha2,
				AddsAndUpdates: []*inpb.File{
					{Filepath: "file1", Content: []byte("file1 content")},
				},
			},
			{
				Sha:       sha4,
				ParentSha: sha3,
				AddsAndUpdates: []*inpb.File{
					{Filepath: "file2", Content: []byte("file2 content")},
				},
				DeleteFilepaths: []string{"file1"},
			},
		},
	}, result)
}

func TestComputeIncrementalUpdate_SkipUnindexable(t *testing.T) {
	firstSHA := "abc123"
	lastSHA := "def456"

	fakeClient := &fakeGitClient{
		t: t,
		commands: map[string]string{
			incrementalLogCommand(firstSHA, lastSHA): `
def456

:100644 100644 bcd1234 0123456 M	file0
`,
		},
		files: map[string][]byte{
			"file0": []byte{0x47, 0x49, 0x46, 0x38, 0x39, 0x61}, // GIF file
		},
	}

	result, err := ComputeIncrementalUpdate(fakeClient, firstSHA, lastSHA)
	require.NoError(t, err)

	assert.Equal(t, &inpb.IncrementalUpdate{
		Commits: []*inpb.Commit{
			{
				Sha:       "def456",
				ParentSha: "abc123",
				// No AddsAndUpdates because the file is unindexable
			},
		},
	}, result)
}

func TestComputeIncrementalUpdate_NoChanges(t *testing.T) {
	firstSHA := "abc123"
	lastSHA := "def456"

	fakeClient := &fakeGitClient{
		t: t,
		commands: map[string]string{
			incrementalLogCommand(firstSHA, lastSHA): "\n",
		},
		files: map[string][]byte{},
	}

	result, err := ComputeIncrementalUpdate(fakeClient, firstSHA, lastSHA)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestComputeIncrementalUpdate_WithWarnings(t *testing.T) {
	firstSHA := "abc123"
	lastSHA := "def456"

	fakeClient := &fakeGitClient{
		t: t,
		commands: map[string]string{
			incrementalLogCommand(firstSHA, lastSHA): `
warning: fetch normally indicates which branches had a forced update,
but that check has been disabled; to re-enable, use '--show-forced-updates'
flag or run 'git config fetch.showForcedUpdates true'
def456

:100644 100644 bcd1234 0123456 M	file0
`,
		},
		files: map[string][]byte{
			"file0": []byte("file0 content"),
		},
	}

	result, err := ComputeIncrementalUpdate(fakeClient, firstSHA, lastSHA)
	require.NoError(t, err)

	assert.Equal(t, &inpb.IncrementalUpdate{
		Commits: []*inpb.Commit{
			{
				Sha:       "def456",
				ParentSha: "abc123",
				AddsAndUpdates: []*inpb.File{
					{Filepath: "file0", Content: []byte("file0 content")},
				},
			},
		},
	}, result)
}
