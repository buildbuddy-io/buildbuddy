package server

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/github"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	spb "github.com/buildbuddy-io/buildbuddy/proto/search"
)

func mustMakeServer(t *testing.T) *codesearchServer {
	tmpDir := testfs.MakeTempDir(t)

	te := real_environment.NewRealEnv(nil)
	te.SetAuthenticator(&nullauth.NullAuthenticator{})

	server, err := New(te, tmpDir, tmpDir)
	require.NoError(t, err)
	return server
}

func bootstrapIndex(t *testing.T, ctx context.Context, server *codesearchServer, repoURL, sha string) {
	// In order to apply incremental updates, we need to first set the last indexed commit
	ns, err := server.getUserNamespace(ctx, "")
	require.NoError(t, err)
	iw, err := index.NewWriter(server.db, ns)
	require.NoError(t, err)
	ru, err := git.ParseGitHubRepoURL(repoURL)
	require.NoError(t, err)
	github.SetRepoMetadata(iw, ru, sha, "")
	err = iw.Flush()
	require.NoError(t, err)
}

func TestIncrementalIndex(t *testing.T) {
	ctx := context.Background()
	server := mustMakeServer(t)

	oldSha := "def456"
	newSha := "abc123"
	bootstrapIndex(t, ctx, server, "github.com/buildbuddy-io/buildbuddy", oldSha)

	rsp, err := server.Index(ctx, &inpb.IndexRequest{
		GitRepo: &gitpb.GitRepo{
			RepoUrl: "github.com/buildbuddy-io/buildbuddy",
		},
		ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
		Update: &inpb.IncrementalUpdate{
			Commits: []*inpb.Commit{
				{
					Sha:       newSha,
					ParentSha: oldSha,
					AddsAndUpdates: []*inpb.File{
						{
							Filepath: "foo/bar/baz.txt",
							Content:  []byte("doo be doo be doooo"),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, rsp)

	repoStatus, err := server.RepoStatus(ctx, &inpb.RepoStatusRequest{
		RepoUrl: "github.com/buildbuddy-io/buildbuddy",
	})
	require.NoError(t, err)
	assert.Equal(t, &inpb.RepoStatusResponse{
		LastIndexedCommitSha: newSha,
	}, repoStatus)

	searchRsp, err := server.Search(ctx, &spb.SearchRequest{
		Query: &spb.Query{
			Term: "doo",
		},
	})
	require.NoError(t, err)

	assert.NotNil(t, searchRsp)
	assert.NotNil(t, searchRsp.Results)
	assert.Len(t, searchRsp.Results, 1)
	search := searchRsp.Results[0]
	search.Snippets = nil // clear out snippets to allow for easier comparison
	assert.Equal(t, &spb.Result{
		Owner:      "buildbuddy-io",
		Repo:       "buildbuddy",
		Sha:        newSha,
		Filename:   "foo/bar/baz.txt",
		MatchCount: 1,
	}, search)
}

func TestIncrementalIndex_SkipSomeCommits(t *testing.T) {
	// This test simulates a scenario where the server has indexed up to commit 3, and a request
	// comes in to index commits 2, 3, and 4. The server should only index commit 4 in this case.

	ctx := context.Background()
	server := mustMakeServer(t)

	commit1 := "a123"
	commit2 := "b456"
	commit3 := "c789"
	commit4 := "d012"

	bootstrapIndex(t, ctx, server, "github.com/buildbuddy-io/buildbuddy", commit3)

	rsp, err := server.Index(ctx, &inpb.IndexRequest{
		GitRepo: &gitpb.GitRepo{
			RepoUrl: "github.com/buildbuddy-io/buildbuddy",
		},
		ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
		Update: &inpb.IncrementalUpdate{
			Commits: []*inpb.Commit{
				{
					Sha:       commit2,
					ParentSha: commit1,
					AddsAndUpdates: []*inpb.File{
						{
							Filepath: "foo/bar/baz.txt",
							Content:  []byte("doo be doo be doooo"),
						},
					},
				},
				{
					Sha:       commit3,
					ParentSha: commit2,
					AddsAndUpdates: []*inpb.File{
						{
							Filepath: "hello/world.txt",
							Content:  []byte("hello world"),
						},
					},
				},
				{
					Sha:       commit4,
					ParentSha: commit3,
					AddsAndUpdates: []*inpb.File{
						{
							Filepath: "goodbye.java",
							Content:  []byte("public static void main(String[] args) { System.out.println(\"Goodbye!\"); }"),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, rsp)

	repoStatus, err := server.RepoStatus(ctx, &inpb.RepoStatusRequest{
		RepoUrl: "github.com/buildbuddy-io/buildbuddy",
	})
	require.NoError(t, err)
	assert.Equal(t, &inpb.RepoStatusResponse{
		LastIndexedCommitSha: commit4,
	}, repoStatus)

	// Look up stuff from commits 2 and 3, which should not have been indexed because we told the index
	// that commit 3 was the last indexed commit.
	searchRsp, err := server.Search(ctx, &spb.SearchRequest{
		Query: &spb.Query{
			Term: "doo",
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, searchRsp)
	assert.Nil(t, searchRsp.Results)

	searchRsp, err = server.Search(ctx, &spb.SearchRequest{
		Query: &spb.Query{
			Term: "hello",
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, searchRsp)
	assert.Nil(t, searchRsp.Results)

	// Now look up stuff from commit 4, it should be there.
	searchRsp, err = server.Search(ctx, &spb.SearchRequest{
		Query: &spb.Query{
			Term: "public",
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, searchRsp)
	assert.NotNil(t, searchRsp.Results)
	assert.Len(t, searchRsp.Results, 1)
	search := searchRsp.Results[0]
	search.Snippets = nil
	assert.Equal(t, &spb.Result{
		Owner:      "buildbuddy-io",
		Repo:       "buildbuddy",
		Sha:        commit4,
		Filename:   "goodbye.java",
		MatchCount: 1,
	}, search)
}

func TestRepoStatus_NoStatus(t *testing.T) {
	server := mustMakeServer(t)

	response, err := server.RepoStatus(context.Background(), &inpb.RepoStatusRequest{
		RepoUrl: "github.com/buildbuddy-io/buildbuddy",
	})
	require.NoError(t, err)
	assert.Equal(t, &inpb.RepoStatusResponse{
		LastIndexedCommitSha: "",
	}, response)
}

func TestRepoStatus_InvalidRepo(t *testing.T) {
	server := mustMakeServer(t)

	response, err := server.RepoStatus(context.Background(), &inpb.RepoStatusRequest{
		RepoUrl: "foobar",
	})
	assert.True(t, status.IsInvalidArgumentError(err))
	assert.Nil(t, response)
}

func TestRepoStatus_NonLinearHistory(t *testing.T) {
	// here we send 2 commits that claim to be children of a123. This should be rejected - commits
	// must be in a linear, non-branching order.
	ctx := context.Background()
	server := mustMakeServer(t)

	bootstrapIndex(t, ctx, server, "github.com/buildbuddy-io/buildbuddy", "a123")

	rsp, err := server.Index(ctx, &inpb.IndexRequest{
		GitRepo: &gitpb.GitRepo{
			RepoUrl: "github.com/buildbuddy-io/buildbuddy",
		},
		ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
		Update: &inpb.IncrementalUpdate{
			Commits: []*inpb.Commit{
				{
					Sha:            "b456",
					ParentSha:      "a123",
					AddsAndUpdates: []*inpb.File{{Filepath: "a", Content: []byte("b")}},
				},
				{
					Sha:            "b456",
					ParentSha:      "a123",
					AddsAndUpdates: []*inpb.File{{Filepath: "c", Content: []byte("d")}},
				},
			},
		},
	})
	log.Errorf("Index response: %v", err)
	assert.True(t, status.IsInvalidArgumentError(err))
	assert.Nil(t, rsp)
}

func TestRepoStatus_OutOfOrder(t *testing.T) {
	// here we send 2 commits out of order. This should be rejected - commits must be in order
	// from oldest ancestor to newest.
	ctx := context.Background()
	server := mustMakeServer(t)

	bootstrapIndex(t, ctx, server, "github.com/buildbuddy-io/buildbuddy", "a123")

	rsp, err := server.Index(ctx, &inpb.IndexRequest{
		GitRepo: &gitpb.GitRepo{
			RepoUrl: "github.com/buildbuddy-io/buildbuddy",
		},
		ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
		Update: &inpb.IncrementalUpdate{
			Commits: []*inpb.Commit{
				{
					Sha:            "c789",
					ParentSha:      "b456",
					AddsAndUpdates: []*inpb.File{{Filepath: "a", Content: []byte("b")}},
				},
				{
					Sha:            "b456",
					ParentSha:      "a123",
					AddsAndUpdates: []*inpb.File{{Filepath: "c", Content: []byte("d")}},
				},
			},
		},
	})
	log.Errorf("Index response: %v", err)
	assert.True(t, status.IsInvalidArgumentError(err))
	assert.Nil(t, rsp)
}

func TestDropNamespace(t *testing.T) {
	// DropNamespace functionality is tested more exhaustively in the index tests,
	// so here we just make sure the plumbing is correct - the request goes through,
	// the generation is committed, etc. We should see that repo status was deleted.

	ctx := context.Background()
	server := mustMakeServer(t)

	sha := "def456"
	bootstrapIndex(t, ctx, server, "github.com/buildbuddy-io/buildbuddy", sha)

	_, err := server.Index(ctx, &inpb.IndexRequest{ReplacementStrategy: inpb.ReplacementStrategy_DROP_NAMESPACE})
	require.NoError(t, err)

	response, err := server.RepoStatus(context.Background(), &inpb.RepoStatusRequest{
		RepoUrl: "github.com/buildbuddy-io/buildbuddy",
	})
	require.NoError(t, err)
	assert.Equal(t, &inpb.RepoStatusResponse{
		LastIndexedCommitSha: "",
	}, response)
}

func makeZip(t *testing.T, files map[string]string) []*zip.File {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)
		_, err = w.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return zr.File
}

func TestModuleFromArchive(t *testing.T) {
	// Archive entries are nested under a single top-level directory, like the
	// GitHub zipball, which moduleFromArchive strips.
	mp, err := moduleFromArchive(makeZip(t, map[string]string{
		"repo-abc123/go.mod":  "module github.com/example/repo\n\ngo 1.24\n",
		"repo-abc123/main.go": "package main\n",
	}))
	require.NoError(t, err)
	assert.Equal(t, "github.com/example/repo", mp)
}

func TestModuleFromArchiveNoGoMod(t *testing.T) {
	mp, err := moduleFromArchive(makeZip(t, map[string]string{
		"repo-abc123/main.go": "package main\n",
	}))
	require.NoError(t, err)
	assert.Empty(t, mp)
}

func seedRepoMetadata(t *testing.T, ctx context.Context, server *codesearchServer, repoURLString, sha, modulePath string) {
	t.Helper()
	ns, err := server.getUserNamespace(ctx, "")
	require.NoError(t, err)
	iw, err := index.NewWriter(server.db, ns)
	require.NoError(t, err)
	ru, err := git.ParseGitHubRepoURL(repoURLString)
	require.NoError(t, err)
	require.NoError(t, github.SetRepoMetadata(iw, ru, sha, modulePath))
	require.NoError(t, iw.Flush())
}

func importMatchCount(t *testing.T, ctx context.Context, server *codesearchServer, importTerm string) int {
	t.Helper()
	ns, err := server.getUserNamespace(ctx, "")
	require.NoError(t, err)
	r := index.NewReader(ctx, server.db, ns, schema.GitHubFileSchema())
	matches, err := r.RawQuery(fmt.Sprintf(`(:eq imports %q)`, importTerm))
	require.NoError(t, err)
	return len(matches)
}

func storedModulePath(t *testing.T, ctx context.Context, server *codesearchServer, repoURLString string) string {
	t.Helper()
	ns, err := server.getUserNamespace(ctx, "")
	require.NoError(t, err)
	r := index.NewReader(ctx, server.db, ns, schema.MetadataSchema())
	ru, err := git.ParseGitHubRepoURL(repoURLString)
	require.NoError(t, err)
	mp, err := github.GetRepoModulePath(r, ru)
	require.NoError(t, err)
	return mp
}

// appImportsLog is a Go file that imports the example repo's util/log package.
const appImportsLog = `package main

import "github.com/example/repo/util/log"

func main() { log.Print() }
`

func TestIncrementalIndexResolvesGoImports(t *testing.T) {
	ctx := t.Context()
	server := mustMakeServer(t)
	repo := "github.com/buildbuddy-io/buildbuddy"
	seedRepoMetadata(t, ctx, server, repo, "old", "github.com/example/repo")

	_, err := server.Index(ctx, &inpb.IndexRequest{
		GitRepo:             &gitpb.GitRepo{RepoUrl: repo},
		ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
		Update: &inpb.IncrementalUpdate{Commits: []*inpb.Commit{{
			Sha: "new", ParentSha: "old",
			AddsAndUpdates: []*inpb.File{
				{Filepath: "app/main.go", Content: []byte(appImportsLog)},
			},
		}}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, importMatchCount(t, ctx, server, "go:github.com/example/repo/util/log"),
		"the stored module path resolves Go import identities on incremental update")
}

func TestIncrementalIndexRefreshesModulePathFromGoMod(t *testing.T) {
	ctx := t.Context()
	server := mustMakeServer(t)
	repo := "github.com/buildbuddy-io/buildbuddy"
	// Seed a stale module path; the commit's go.mod should override it.
	seedRepoMetadata(t, ctx, server, repo, "old", "stale/module")

	_, err := server.Index(ctx, &inpb.IndexRequest{
		GitRepo:             &gitpb.GitRepo{RepoUrl: repo},
		ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
		Update: &inpb.IncrementalUpdate{Commits: []*inpb.Commit{{
			Sha: "new", ParentSha: "old",
			AddsAndUpdates: []*inpb.File{
				{Filepath: "go.mod", Content: []byte("module github.com/example/repo\n\ngo 1.24\n")},
				{Filepath: "app/main.go", Content: []byte(appImportsLog)},
			},
		}}},
	})
	require.NoError(t, err)
	assert.Equal(t, "github.com/example/repo", storedModulePath(t, ctx, server, repo),
		"a commit touching go.mod refreshes the stored module path")
	assert.Equal(t, 1, importMatchCount(t, ctx, server, "go:github.com/example/repo/util/log"),
		"imports resolve against the refreshed module path, not the stale one")
}

func TestIncrementalIndexIgnoresUnparsableGoMod(t *testing.T) {
	ctx := t.Context()
	server := mustMakeServer(t)
	repo := "github.com/buildbuddy-io/buildbuddy"
	seedRepoMetadata(t, ctx, server, repo, "old", "github.com/example/repo")

	_, err := server.Index(ctx, &inpb.IndexRequest{
		GitRepo:             &gitpb.GitRepo{RepoUrl: repo},
		ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
		Update: &inpb.IncrementalUpdate{Commits: []*inpb.Commit{{
			Sha: "new", ParentSha: "old",
			AddsAndUpdates: []*inpb.File{
				{Filepath: "go.mod", Content: []byte("this is not a valid go.mod")},
			},
		}}},
	})
	require.NoError(t, err)
	assert.Equal(t, "github.com/example/repo", storedModulePath(t, ctx, server, repo),
		"an unparsable go.mod must not wipe the stored module path")
}
