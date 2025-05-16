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
	repoURL1, err := git.ParseGitHubRepoURL("github.com/buildbuddy-io/buildbuddy")
	require.NoError(t, err)

	repoURL2, err := git.ParseGitHubRepoURL("github.com/buildbuddy-io/buildbuddy-internal")
	require.NoError(t, err)

	r := index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err := GetLastIndexedCommitSha(r, repoURL2)
	require.NoError(t, err)
	assert.Empty(t, lastRev)

	// Set a different repo, and make sure we still don't find repo2
	w, err := index.NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	assert.NoError(t, SetLastIndexedCommitSha(w, repoURL1, commitSHA))
	require.NoError(t, w.Flush())

	r = index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err = GetLastIndexedCommitSha(r, repoURL2)
	require.NoError(t, err)
	assert.Empty(t, lastRev)
}
