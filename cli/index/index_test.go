package index

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/github"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	gpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
)

// This test client implements the CodeSearch service methods. It has a hardcoded lastCommitSHA,
// and records Index requests sent to it.
type testBBClient struct {
	bbspb.BuildBuddyServiceClient
	latestCommitSHA string
	indexReqs       []*inpb.IndexRequest
}

func (c *testBBClient) RepoStatus(ctx context.Context, req *inpb.RepoStatusRequest, opts ...grpc.CallOption) (*inpb.RepoStatusResponse, error) {
	return &inpb.RepoStatusResponse{
		LastIndexedCommitSha: c.latestCommitSHA,
	}, nil
}

func (c *testBBClient) Index(ctx context.Context, in *inpb.IndexRequest, opts ...grpc.CallOption) (*inpb.IndexResponse, error) {
	// Simulate indexing by just returning the request as the response.
	if c.indexReqs == nil {
		c.indexReqs = []*inpb.IndexRequest{}
	}
	c.indexReqs = append(c.indexReqs, in)
	return &inpb.IndexResponse{}, nil
}

func TestIncremental_OneCommit(t *testing.T) {
	scratchDir, initialSHA := testgit.MakeTempRepo(t, map[string]string{
		"README.md": "This is a test repo",
	})

	repoUrl := "https://github.com/buildbuddy-io/buildbuddy"
	testgit.ConfigureRemoteOrigin(t, scratchDir, repoUrl)

	commit1 := testgit.CommitFiles(t, scratchDir, map[string]string{
		"test.txt": "initial content",
	})

	gc := github.NewCommandLineGitClient(scratchDir)
	client := &testBBClient{
		latestCommitSHA: initialSHA,
	}

	require.NoError(t, indexRepo(gc, client))
	assert.Equal(t, []*inpb.IndexRequest{
		{
			GitRepo: &gpb.GitRepo{
				RepoUrl:  repoUrl,
				Username: "buildbuddy-io",
			},
			ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
			Update: &inpb.IncrementalUpdate{
				Commits: []*inpb.Commit{
					{
						Sha:       commit1,
						ParentSha: initialSHA,
						AddsAndUpdates: []*inpb.File{
							{
								Filepath: "test.txt",
								Content:  []byte("initial content"),
							},
						},
					},
				},
			},
		},
	}, client.indexReqs)
}

func TestIncremental_TwoCommits(t *testing.T) {
	scratchDir, initialSHA := testgit.MakeTempRepo(t, map[string]string{
		"README.md": "This is a test repo",
	})

	repoUrl := "https://github.com/buildbuddy-io/buildbuddy"
	testgit.ConfigureRemoteOrigin(t, scratchDir, repoUrl)

	commit1 := testgit.CommitFiles(t, scratchDir, map[string]string{
		"test.txt": "initial content",
	})

	commit2 := testgit.CommitFiles(t, scratchDir, map[string]string{
		"test2.txt": "better content",
	})

	gc := github.NewCommandLineGitClient(scratchDir)
	client := &testBBClient{
		latestCommitSHA: initialSHA,
	}

	require.NoError(t, indexRepo(gc, client))
	assert.Equal(t, []*inpb.IndexRequest{
		{
			GitRepo: &gpb.GitRepo{
				RepoUrl:  repoUrl,
				Username: "buildbuddy-io",
			},
			ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
			Update: &inpb.IncrementalUpdate{
				Commits: []*inpb.Commit{
					{
						Sha:       commit1,
						ParentSha: initialSHA,
						AddsAndUpdates: []*inpb.File{
							{
								Filepath: "test.txt",
								Content:  []byte("initial content"),
							},
						},
					},
					{
						Sha:       commit2,
						ParentSha: commit1,
						AddsAndUpdates: []*inpb.File{
							{
								Filepath: "test2.txt",
								Content:  []byte("better content"),
							},
						},
					},
				},
			},
		},
	}, client.indexReqs)
}

func TestIncremental_NoOp(t *testing.T) {
	// In this test, we have extra commits, but RepoStatus will report that we have
	// already indexed them.
	scratchDir, initialCommit := testgit.MakeTempRepo(t, map[string]string{
		"README.md": "This is a test repo",
	})

	repoUrl := "https://github.com/buildbuddy-io/buildbuddy"
	testgit.ConfigureRemoteOrigin(t, scratchDir, repoUrl)

	commit1 := testgit.CommitFiles(t, scratchDir, map[string]string{
		"test.txt": "initial content",
	})

	commit2 := testgit.CommitFiles(t, scratchDir, map[string]string{
		"test2.txt": "better content",
	})

	gc := github.NewCommandLineGitClient(scratchDir)
	client := &testBBClient{
		latestCommitSHA: commit2,
	}
	require.NoError(t, indexRepo(gc, client))
	assert.Empty(t, client.indexReqs)

	testshell.Run(t, scratchDir, `git reset --hard `+initialCommit)
	gc = github.NewCommandLineGitClient(scratchDir)
	client = &testBBClient{
		latestCommitSHA: commit2,
	}
	require.NoError(t, indexRepo(gc, client))
	assert.Empty(t, client.indexReqs)

	testshell.Run(t, scratchDir, `git reset --hard `+commit1)
	gc = github.NewCommandLineGitClient(scratchDir)
	client = &testBBClient{
		latestCommitSHA: commit2,
	}
	require.NoError(t, indexRepo(gc, client))
	assert.Empty(t, client.indexReqs)
}

func TestIncremental_TooBig(t *testing.T) {
	scratchDir, initialCommit := testgit.MakeTempRepo(t, map[string]string{
		"README.md": "This is a test repo",
	})

	repoUrl := "https://github.com/buildbuddy-io/buildbuddy"
	testgit.ConfigureRemoteOrigin(t, scratchDir, repoUrl)

	filesToAdd := make(map[string]string, 1100)
	for i := 0; i < 1100; i++ {
		filesToAdd[fmt.Sprintf("test%d.txt", i)] = fmt.Sprintf("content %d", i)
	}
	commit1 := testgit.CommitFiles(t, scratchDir, filesToAdd)

	gc := github.NewCommandLineGitClient(scratchDir)
	client := &testBBClient{
		latestCommitSHA: initialCommit,
	}
	require.NoError(t, indexRepo(gc, client))
	assert.Equal(t, []*inpb.IndexRequest{
		{
			GitRepo: &gpb.GitRepo{
				RepoUrl:  repoUrl,
				Username: "buildbuddy-io",
			},
			RepoState: &gpb.RepoState{
				CommitSha: commit1,
			},
			Async:               true,
			ReplacementStrategy: inpb.ReplacementStrategy_REPLACE_REPO,
		},
	}, client.indexReqs)

}

func TestIncremental_NoPreviousIndex(t *testing.T) {
	scratchDir, _ := testgit.MakeTempRepo(t, map[string]string{
		"README.md": "This is a test repo",
	})

	repoUrl := "https://github.com/buildbuddy-io/buildbuddy"
	testgit.ConfigureRemoteOrigin(t, scratchDir, repoUrl)

	commit1 := testgit.CommitFiles(t, scratchDir, map[string]string{
		"test.txt": "initial content",
	})

	gc := github.NewCommandLineGitClient(scratchDir)
	client := &testBBClient{
		latestCommitSHA: "",
	}
	require.NoError(t, indexRepo(gc, client))
	assert.Equal(t, []*inpb.IndexRequest{
		{
			GitRepo: &gpb.GitRepo{
				RepoUrl:  repoUrl,
				Username: "buildbuddy-io",
			},
			RepoState: &gpb.RepoState{
				CommitSha: commit1,
			},
			Async:               true,
			ReplacementStrategy: inpb.ReplacementStrategy_REPLACE_REPO,
		},
	}, client.indexReqs)
}
