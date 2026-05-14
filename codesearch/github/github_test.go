package github

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
)

var (
	repoURL    = &git.RepoURL{Host: "github.com", Owner: "buildbuddy-io", Repo: "buildbuddy"}
	altRepoURL = &git.RepoURL{Host: "github.com", Owner: "buildbuddy-io", Repo: "buildbuddy-internal"}
)

func mustOpenDB(t *testing.T, indexDir string) *pebble.DB {
	t.Helper()
	db, err := index.OpenPebbleDB(indexDir)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestLastIndexedCommit(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	commitSHA := "abc123"

	w, err := index.NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	assert.NoError(t, SetLastIndexedCommitSha(w, repoURL, commitSHA))
	require.NoError(t, w.Flush())

	r := index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err := GetLastIndexedCommitSha(r, repoURL)
	require.NoError(t, err)
	assert.Equal(t, commitSHA, lastRev)
}

func TestLastIndexedCommitUpdated(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	commitSHA := "abc123"
	repoURL, err := git.ParseGitHubRepoURL("github.com/buildbuddy-io/buildbuddy")
	require.NoError(t, err)

	w, err := index.NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	assert.NoError(t, SetLastIndexedCommitSha(w, repoURL, commitSHA))
	require.NoError(t, w.Flush())

	r := index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err := GetLastIndexedCommitSha(r, repoURL)
	require.NoError(t, err)
	assert.Equal(t, commitSHA, lastRev)

	w, err = index.NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	commitSHA2 := "def456"
	assert.NoError(t, SetLastIndexedCommitSha(w, repoURL, commitSHA2))
	require.NoError(t, w.Flush())

	r = index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err = GetLastIndexedCommitSha(r, repoURL)
	require.NoError(t, err)
	assert.Equal(t, commitSHA2, lastRev)

}

func TestLastIndexedCommitUnset(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	commitSHA := "abc123"

	r := index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err := GetLastIndexedCommitSha(r, altRepoURL)
	assert.True(t, status.IsNotFoundError(err))
	assert.Empty(t, lastRev)

	// Set a different repo, and make sure we still don't find repo2
	w, err := index.NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	assert.NoError(t, SetLastIndexedCommitSha(w, repoURL, commitSHA))
	require.NoError(t, w.Flush())

	r = index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err = GetLastIndexedCommitSha(r, altRepoURL)
	assert.True(t, status.IsNotFoundError(err))
	assert.Empty(t, lastRev)
}

func TestAddFileToIndex(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := index.NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}

	commitSHA := "abc123"
	filepath := "foo/bar/baz.go"
	fileContent := []byte("package foo\n\nfunc Bar() {}\n")

	err = AddFileToIndex(w, repoURL, commitSHA, filepath, fileContent)
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	r := index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc := r.GetStoredDocument(1)

	assert.Equal(t, "foo/bar/baz.go", string(doc.Field(schema.FilenameField).Contents()))
	assert.Equal(t, "package foo\n\nfunc Bar() {}\n", string(doc.Field(schema.ContentField).Contents()))
	assert.Equal(t, "go", string(doc.Field(schema.LanguageField).Contents()))
	assert.Equal(t, "buildbuddy-io", string(doc.Field(schema.OwnerField).Contents()))
	assert.Equal(t, "buildbuddy", string(doc.Field(schema.RepoField).Contents()))
	assert.Equal(t, "abc123", string(doc.Field(schema.SHAField).Contents()))
}

func TestAddFileToIndexWrongMimeType(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := index.NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}

	commitSHA := "abc123"
	filepath := "foo/bar/baz.gif"
	fileContent := []byte{0x47, 0x49, 0x46, 0x38, 0x39, 0x61} // GIF file

	err = AddFileToIndex(w, repoURL, commitSHA, filepath, fileContent)
	assert.Error(t, err)

	r := index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc := r.GetStoredDocument(1)

	assert.Empty(t, string(doc.Field(schema.FilenameField).Contents()))
}

func TestAddFileToIndexInvalidUTF8(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := index.NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}

	commitSHA := "abc123"
	filepath := "foo/bar/baz.go"
	fileContent := []byte{0xF0, 0xA4, 0xAD} // invalid UTF-8

	err = AddFileToIndex(w, repoURL, commitSHA, filepath, fileContent)
	assert.Error(t, err)

	r := index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc := r.GetStoredDocument(1)

	assert.Empty(t, string(doc.Field(schema.FilenameField).Contents()))
}

func mustApplyCommit(t *testing.T, db *pebble.DB, commit *inpb.Commit) {
	w, err := index.NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	err = ProcessCommit(w, repoURL, commit)
	require.NoError(t, err)
	require.NoError(t, w.Flush())
}

func TestProcessCommit_AddsOnly(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	mustApplyCommit(t, db, &inpb.Commit{
		Sha: "abc123",
		AddsAndUpdates: []*inpb.File{
			{Filepath: "foo/bar/baz.go", Content: []byte("package foo\n\nfunc Bar() {}\n")},
		},
	})

	r := index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc := r.GetStoredDocument(1)
	assert.Equal(t, "foo/bar/baz.go", string(doc.Field(schema.FilenameField).Contents()))
	assert.Equal(t, "package foo\n\nfunc Bar() {}\n", string(doc.Field(schema.ContentField).Contents()))
}

func TestProcessCommit_DeletesOnly(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	// Add a file first
	mustApplyCommit(t, db, &inpb.Commit{
		Sha: "abc123",
		AddsAndUpdates: []*inpb.File{
			{Filepath: "foo/bar/baz.go", Content: []byte("package foo\n\nfunc Bar() {}\n")},
		},
	})
	r := index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc := r.GetStoredDocument(1)
	assert.Equal(t, "package foo\n\nfunc Bar() {}\n", string(doc.Field(schema.ContentField).Contents()))

	// Now delete the file
	mustApplyCommit(t, db, &inpb.Commit{
		Sha:             "def456",
		DeleteFilepaths: []string{"foo/bar/baz.go"},
	})

	r = index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc = r.GetStoredDocument(1)
	assert.Empty(t, doc.Field(schema.ContentField).Contents())
}

func TestProcessCommit_DeleteThenReAdd(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	filename := "foo/bar/baz.go"

	// Add a file first
	mustApplyCommit(t, db, &inpb.Commit{
		Sha: "abc123",
		AddsAndUpdates: []*inpb.File{
			{Filepath: filename, Content: []byte("package foo\n\nfunc Bar() {}\n")},
		},
	})
	r := index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc := r.GetStoredDocument(1)
	assert.Equal(t, "package foo\n\nfunc Bar() {}\n", string(doc.Field(schema.ContentField).Contents()))

	// Now delete the file
	mustApplyCommit(t, db, &inpb.Commit{
		Sha:             "def456",
		DeleteFilepaths: []string{filename},
	})

	r = index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc = r.GetStoredDocument(1)
	assert.Empty(t, doc.Field(schema.ContentField).Contents())

	// Add it again
	mustApplyCommit(t, db, &inpb.Commit{
		Sha: "ghi789",
		AddsAndUpdates: []*inpb.File{
			{Filepath: filename, Content: []byte("package baz\n\nfunc Beetle() {}\n")},
		},
	})

	r = index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc = r.GetStoredDocument(2<<32 | 1)
	assert.Equal(t, "package baz\n\nfunc Beetle() {}\n", string(doc.Field(schema.ContentField).Contents()))
}

func TestProcessCommit_NonOverlappingAddsAndDeletes(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	mustApplyCommit(t, db, &inpb.Commit{
		Sha: "abc123",
		AddsAndUpdates: []*inpb.File{
			{Filepath: "foo/bar/baz.go", Content: []byte("package foo\n\nfunc Bar() {}\n")},
		},
		DeleteFilepaths: []string{"foo/bar/old.go"},
	})

	r := index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc := r.GetStoredDocument(1)
	assert.Equal(t, "foo/bar/baz.go", string(doc.Field(schema.FilenameField).Contents()))
}

func TestProcessCommit_OverlappingAddsAndDeletes(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	filename := "foo/bar/baz.go"

	mustApplyCommit(t, db, &inpb.Commit{
		Sha: "abc123",
		AddsAndUpdates: []*inpb.File{
			{Filepath: filename, Content: []byte("package foo\n\nfunc Bar() {}\n")},
		},
	})

	r := index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc := r.GetStoredDocument(1)
	require.Equal(t, filename, string(doc.Field(schema.FilenameField).Contents()))

	mustApplyCommit(t, db, &inpb.Commit{
		Sha: "def456",
		AddsAndUpdates: []*inpb.File{
			{Filepath: filename, Content: []byte("package baz\n\nfunc Beetle() {}\n")},
		},
		DeleteFilepaths: []string{filename},
	})

	r = index.NewReader(ctx, db, "testing-namespace", schema.GitHubFileSchema())
	doc = r.GetStoredDocument(1)
	assert.Empty(t, doc.Field(schema.ContentField).Contents())

	doc = r.GetStoredDocument(1<<32 | 1)
	assert.Equal(t, "package baz\n\nfunc Beetle() {}\n", string(doc.Field(schema.ContentField).Contents()))
}

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
