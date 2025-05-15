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
	if err != nil {
		t.Fatal(err)
	}

	w, err := index.NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}

	assert.NoError(t, SetLastIndexedCommitSha(w, repoURL, commitSHA))
	require.NoError(t, w.Flush())

	r := index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err := GetLastIndexedCommitSha(r, repoURL)
	require.NoError(t, err)
	assert.Equal(t, commitSHA, lastRev)
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
	if err != nil {
		t.Fatal(err)
	}

	repoURL2, err := git.ParseGitHubRepoURL("github.com/buildbuddy-io/buildbuddy-internal")
	if err != nil {
		t.Fatal(err)
	}

	r := index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err := GetLastIndexedCommitSha(r, repoURL2)
	require.NoError(t, err)
	assert.Empty(t, lastRev)

	// Set a different repo, and make sure we still don't find repo2
	w, err := index.NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}
	assert.NoError(t, SetLastIndexedCommitSha(w, repoURL1, commitSHA))
	require.NoError(t, w.Flush())

	r = index.NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	lastRev, err = GetLastIndexedCommitSha(r, repoURL2)
	require.NoError(t, err)
	assert.Empty(t, lastRev)
}
