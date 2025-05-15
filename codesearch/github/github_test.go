package github

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
)

var (
	repoURL    = &git.RepoURL{Host: "github.com", Owner: "buildbuddy-io", Repo: "buildbuddy"}
	altRepoURL = &git.RepoURL{Host: "github.com", Owner: "buildbuddy-io", Repo: "buildbuddy-internal"}
)

func TestLastIndexedCommit(t *testing.T) {
	ctx := context.Background()
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	commitSHA := "abc123"

	r := index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err := GetLastIndexedCommitSha(r, altRepoURL)
	require.NoError(t, err)
	assert.Empty(t, lastRev)

	// Set a different repo, and make sure we still don't find repo2
	w, err := index.NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	assert.NoError(t, SetLastIndexedCommitSha(w, repoURL, commitSHA))
	require.NoError(t, w.Flush())

	r = index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err = GetLastIndexedCommitSha(r, altRepoURL)
	require.NoError(t, err)
	assert.Empty(t, lastRev)
}

func TestAddFileToIndex(t *testing.T) {
	ctx := context.Background()
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	w, err := index.NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}

	commitSHA := "abc123"
	filepath := "foo/bar/baz.go"
	fileContent := []byte("package foo\n\nfunc Bar() {}\n")

	err = AddFileToIndex(w, repoURL, commitSHA, filepath, fileContent)
	if err != nil {
		t.Fatalf("AddFileToIndex() failed: %v", err)
	}
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

func mustApplyCommit(t *testing.T, db *pebble.DB, commit *inpb.Commit) {
	w, err := index.NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	err = ProcessCommit(w, repoURL, commit)
	require.NoError(t, err)
	require.NoError(t, w.Flush())
}

// TODO(jdelfino): review tests
func TestProcessCommit_AddsOnly(t *testing.T) {
	ctx := context.Background()
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	require.NoError(t, err)
	defer db.Close()

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
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	require.NoError(t, err)
	defer db.Close()

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
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	require.NoError(t, err)
	defer db.Close()

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
	// TODO(jdelfino): the doc is still there, this function doesn't respect the delete list
	// should it? or should it just be deleted?
	// then GetLastIndexedCommitSha would need to run a real query... which is fine.
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
	doc = r.GetStoredDocument(1)
	assert.Equal(t, "package baz\n\nfunc Beetle() {}\n", string(doc.Field(schema.ContentField).Contents()))
}

func TestProcessCommit_NonOverlappingAddsAndDeletes(t *testing.T) {
	ctx := context.Background()
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	require.NoError(t, err)
	defer db.Close()

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
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	require.NoError(t, err)
	defer db.Close()

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

	doc = r.GetStoredDocument(2<<32 | 1)
	require.NoError(t, err)
	assert.Equal(t, "package baz\n\nfunc Beetle() {}\n", string(doc.Field(schema.ContentField).Contents()))
}
