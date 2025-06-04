package server

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/github"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
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
	github.SetLastIndexedCommitSha(iw, ru, sha)
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
