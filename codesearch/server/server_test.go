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
	xsrv "kythe.io/kythe/go/services/xrefs"
	gsrv "kythe.io/kythe/go/serving/graph"
	gpb "kythe.io/kythe/proto/graph_go_proto"
	xrefpb "kythe.io/kythe/proto/xref_go_proto"
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

type TestTable struct {
	gsrv.Table

	response         *gpb.EdgesReply
	requestedTickets []string
}

func (tt *TestTable) Edges(ctx context.Context, edgeReq *gpb.EdgesRequest) (*gpb.EdgesReply, error) {
	tt.requestedTickets = edgeReq.GetTicket()
	return tt.response, nil
}

type TestXrefService struct {
	xsrv.Service

	response         *xrefpb.CrossReferencesReply
	requestedTickets []string
}

func (xs *TestXrefService) CrossReferences(ctx context.Context, req *xrefpb.CrossReferencesRequest) (*xrefpb.CrossReferencesReply, error) {
	xs.requestedTickets = req.GetTicket()
	return xs.response, nil
}

func TestUsage_Override(t *testing.T) {
	// In this test, A overrides B. If A overrides B, we should see:
	// 1. A's definition in Definitions
	// 2. B's definition in Overrides
	// 3. References to both A and B in References

	ctx := context.Background()

	ticketA := "kythe://test?path=a"
	ticketB := "kythe://test?path=b"

	// These canned responses don't have all fields filled in, but should have everything needed
	// directly by the function under test.

	edgeResp := &gpb.EdgesReply{
		EdgeSets: map[string]*gpb.EdgeSet{
			ticketA: {
				Groups: map[string]*gpb.EdgeSet_Group{
					"/kythe/edge/overrides": {
						Edge: []*gpb.EdgeSet_Group_Edge{
							{TargetTicket: ticketB},
						},
					},
				},
			},
		},
	}

	xrefResp := &xrefpb.CrossReferencesReply{
		CrossReferences: map[string]*xrefpb.CrossReferencesReply_CrossReferenceSet{
			ticketA: {
				Ticket: ticketA,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a ref"}},
				},
			},
			ticketB: {
				Ticket: ticketB,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b ref"}},
				},
			},
		},
	}

	testTable := &TestTable{
		response: edgeResp,
	}
	testXrefService := &TestXrefService{
		response: xrefResp,
	}
	css := &codesearchServer{
		gs: testTable,
		xs: testXrefService,
	}

	rsp, err := css.KytheProxy(ctx, &spb.KytheRequest{
		Value: &spb.KytheRequest_ExtendedXrefsRequest{
			ExtendedXrefsRequest: &spb.ExtendedXrefsRequest{
				Tickets: []string{ticketA},
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, rsp)

	assert.Equal(t, []string{ticketA}, testTable.requestedTickets)
	assert.Equal(t, []string{ticketA, ticketB}, testXrefService.requestedTickets)

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a defn"}},
	}, rsp.GetExtendedXrefsReply().GetDefinitions())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "b defn"}},
	}, rsp.GetExtendedXrefsReply().GetOverrides())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a ref"}},
		{Anchor: &xrefpb.Anchor{Text: "b ref"}},
	}, rsp.GetExtendedXrefsReply().GetReferences())

	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverriddenBy())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtends())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtendedBy())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetGeneratedBy())
}

func TestUsage_OverridenBy(t *testing.T) {
	// In this test, A is overriden by B. If A is overridden by B, we should see:
	// 1. A's definition in Definitions
	// 2. B's definition in Overrides
	// 3. References to both A and B in References

	ctx := context.Background()

	ticketA := "kythe://test?path=a"
	ticketB := "kythe://test?path=b"

	// These canned responses don't have all fields filled in, but should have everything needed
	// directly by the function under test.

	edgeResp := &gpb.EdgesReply{
		EdgeSets: map[string]*gpb.EdgeSet{
			ticketA: {
				Groups: map[string]*gpb.EdgeSet_Group{
					"%/kythe/edge/overrides": {
						Edge: []*gpb.EdgeSet_Group_Edge{
							{TargetTicket: ticketB},
						},
					},
				},
			},
		},
	}

	xrefResp := &xrefpb.CrossReferencesReply{
		CrossReferences: map[string]*xrefpb.CrossReferencesReply_CrossReferenceSet{
			ticketA: {
				Ticket: ticketA,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a ref"}},
				},
			},
			ticketB: {
				Ticket: ticketB,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b ref"}},
				},
			},
		},
	}

	testTable := &TestTable{
		response: edgeResp,
	}
	testXrefService := &TestXrefService{
		response: xrefResp,
	}
	css := &codesearchServer{
		gs: testTable,
		xs: testXrefService,
	}

	rsp, err := css.KytheProxy(ctx, &spb.KytheRequest{
		Value: &spb.KytheRequest_ExtendedXrefsRequest{
			ExtendedXrefsRequest: &spb.ExtendedXrefsRequest{
				Tickets: []string{ticketA},
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, rsp)

	assert.Equal(t, []string{ticketA}, testTable.requestedTickets)
	assert.Equal(t, []string{ticketA, ticketB}, testXrefService.requestedTickets)

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a defn"}},
	}, rsp.GetExtendedXrefsReply().GetDefinitions())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "b defn"}},
	}, rsp.GetExtendedXrefsReply().GetOverriddenBy())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a ref"}},
		{Anchor: &xrefpb.Anchor{Text: "b ref"}},
	}, rsp.GetExtendedXrefsReply().GetReferences())

	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverrides())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtends())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtendedBy())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetGeneratedBy())
}

func TestUsage_Extends(t *testing.T) {
	// In this test, A extends B. If A extends B, we should see:
	// 1. A's definition in Definitions
	// 2. B's definition in Extends
	// 3. References to A and B in References

	ctx := context.Background()

	ticketA := "kythe://test?path=a"
	ticketB := "kythe://test?path=b"

	// These canned responses don't have all fields filled in, but should have everything needed
	// directly by the function under test.

	edgeResp := &gpb.EdgesReply{
		EdgeSets: map[string]*gpb.EdgeSet{
			ticketA: {
				Groups: map[string]*gpb.EdgeSet_Group{
					"/kythe/edge/satisfies": {
						Edge: []*gpb.EdgeSet_Group_Edge{
							{TargetTicket: ticketB},
						},
					},
				},
			},
		},
	}

	xrefResp := &xrefpb.CrossReferencesReply{
		CrossReferences: map[string]*xrefpb.CrossReferencesReply_CrossReferenceSet{
			ticketA: {
				Ticket: ticketA,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a ref"}},
				},
			},
			ticketB: {
				Ticket: ticketB,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b ref"}},
				},
			},
		},
	}

	testTable := &TestTable{
		response: edgeResp,
	}
	testXrefService := &TestXrefService{
		response: xrefResp,
	}
	css := &codesearchServer{
		gs: testTable,
		xs: testXrefService,
	}

	rsp, err := css.KytheProxy(ctx, &spb.KytheRequest{
		Value: &spb.KytheRequest_ExtendedXrefsRequest{
			ExtendedXrefsRequest: &spb.ExtendedXrefsRequest{
				Tickets: []string{ticketA},
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, rsp)

	assert.Equal(t, []string{ticketA}, testTable.requestedTickets)
	assert.Equal(t, []string{ticketA, ticketB}, testXrefService.requestedTickets)

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a defn"}},
	}, rsp.GetExtendedXrefsReply().GetDefinitions())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "b defn"}},
	}, rsp.GetExtendedXrefsReply().GetExtends())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a ref"}},
		{Anchor: &xrefpb.Anchor{Text: "b ref"}},
	}, rsp.GetExtendedXrefsReply().GetReferences())

	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverrides())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverriddenBy())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtendedBy())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetGeneratedBy())
}

func TestUsage_ExtendedBy(t *testing.T) {
	// In this test, A is extended by B. If A is extended by B, we should see:
	// 1. A's definition in Definitions
	// 2. B's definition in ExtendedBy
	// 3. References to A and B in References

	ctx := context.Background()

	ticketA := "kythe://test?path=a"
	ticketB := "kythe://test?path=b"

	// These canned responses don't have all fields filled in, but should have everything needed
	// directly by the function under test.

	edgeResp := &gpb.EdgesReply{
		EdgeSets: map[string]*gpb.EdgeSet{
			ticketA: {
				Groups: map[string]*gpb.EdgeSet_Group{
					"%/kythe/edge/extends": {
						Edge: []*gpb.EdgeSet_Group_Edge{
							{TargetTicket: ticketB},
						},
					},
				},
			},
		},
	}

	xrefResp := &xrefpb.CrossReferencesReply{
		CrossReferences: map[string]*xrefpb.CrossReferencesReply_CrossReferenceSet{
			ticketA: {
				Ticket: ticketA,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a ref"}},
				},
			},
			ticketB: {
				Ticket: ticketB,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b ref"}},
				},
			},
		},
	}

	testTable := &TestTable{
		response: edgeResp,
	}
	testXrefService := &TestXrefService{
		response: xrefResp,
	}
	css := &codesearchServer{
		gs: testTable,
		xs: testXrefService,
	}

	rsp, err := css.KytheProxy(ctx, &spb.KytheRequest{
		Value: &spb.KytheRequest_ExtendedXrefsRequest{
			ExtendedXrefsRequest: &spb.ExtendedXrefsRequest{
				Tickets: []string{ticketA},
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, rsp)

	assert.Equal(t, []string{ticketA}, testTable.requestedTickets)
	assert.Equal(t, []string{ticketA, ticketB}, testXrefService.requestedTickets)

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a defn"}},
	}, rsp.GetExtendedXrefsReply().GetDefinitions())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "b defn"}},
	}, rsp.GetExtendedXrefsReply().GetExtendedBy())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a ref"}},
		{Anchor: &xrefpb.Anchor{Text: "b ref"}},
	}, rsp.GetExtendedXrefsReply().GetReferences())

	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverrides())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverriddenBy())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtends())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetGeneratedBy())

}

func TestUsage_Generates(t *testing.T) {
	// In this test, A generates B. If A generates B, we should see:
	// 1. A's definition in Definitions
	// 2. References to A and B in References

	ctx := context.Background()

	ticketA := "kythe://test?path=a"
	ticketB := "kythe://test?path=b"

	// These canned responses don't have all fields filled in, but should have everything needed
	// directly by the function under test.

	edgeResp := &gpb.EdgesReply{
		EdgeSets: map[string]*gpb.EdgeSet{
			ticketA: {
				Groups: map[string]*gpb.EdgeSet_Group{
					"/kythe/edge/generates": {
						Edge: []*gpb.EdgeSet_Group_Edge{
							{TargetTicket: ticketB},
						},
					},
				},
			},
		},
	}

	xrefResp := &xrefpb.CrossReferencesReply{
		CrossReferences: map[string]*xrefpb.CrossReferencesReply_CrossReferenceSet{
			ticketA: {
				Ticket: ticketA,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a ref"}},
				},
			},
			ticketB: {
				Ticket: ticketB,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b ref"}},
				},
			},
		},
	}

	testTable := &TestTable{
		response: edgeResp,
	}
	testXrefService := &TestXrefService{
		response: xrefResp,
	}
	css := &codesearchServer{
		gs: testTable,
		xs: testXrefService,
	}

	rsp, err := css.KytheProxy(ctx, &spb.KytheRequest{
		Value: &spb.KytheRequest_ExtendedXrefsRequest{
			ExtendedXrefsRequest: &spb.ExtendedXrefsRequest{
				Tickets: []string{ticketA},
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, rsp)

	assert.Equal(t, []string{ticketA}, testTable.requestedTickets)
	assert.Equal(t, []string{ticketA, ticketB}, testXrefService.requestedTickets)

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a defn"}},
	}, rsp.GetExtendedXrefsReply().GetDefinitions())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a ref"}},
		{Anchor: &xrefpb.Anchor{Text: "b ref"}},
	}, rsp.GetExtendedXrefsReply().GetReferences())

	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverrides())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverriddenBy())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtends())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtendedBy())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetGeneratedBy())
}

func TestUsage_GeneratedBy(t *testing.T) {
	// In this test, A is generated by B. If A is generated by B, we should see:
	// 1. A's definition in Definitions
	// 2. B's definition in GeneratedBy
	// 2. References to A in References

	ctx := context.Background()

	ticketA := "kythe://test?path=a"
	ticketB := "kythe://test?path=b"

	// These canned responses don't have all fields filled in, but should have everything needed
	// directly by the function under test.

	edgeResp := &gpb.EdgesReply{
		EdgeSets: map[string]*gpb.EdgeSet{
			ticketA: {
				Groups: map[string]*gpb.EdgeSet_Group{
					"%/kythe/edge/generates": {
						Edge: []*gpb.EdgeSet_Group_Edge{
							{TargetTicket: ticketB},
						},
					},
				},
			},
		},
	}

	xrefResp := &xrefpb.CrossReferencesReply{
		CrossReferences: map[string]*xrefpb.CrossReferencesReply_CrossReferenceSet{
			ticketA: {
				Ticket: ticketA,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "a ref"}},
				},
			},
			ticketB: {
				Ticket: ticketB,
				Definition: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b defn"}},
				},
				Reference: []*xrefpb.CrossReferencesReply_RelatedAnchor{
					{Anchor: &xrefpb.Anchor{Text: "b ref"}},
				},
			},
		},
	}

	testTable := &TestTable{
		response: edgeResp,
	}
	testXrefService := &TestXrefService{
		response: xrefResp,
	}
	css := &codesearchServer{
		gs: testTable,
		xs: testXrefService,
	}

	rsp, err := css.KytheProxy(ctx, &spb.KytheRequest{
		Value: &spb.KytheRequest_ExtendedXrefsRequest{
			ExtendedXrefsRequest: &spb.ExtendedXrefsRequest{
				Tickets: []string{ticketA},
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, rsp)

	assert.Equal(t, []string{ticketA}, testTable.requestedTickets)
	assert.Equal(t, []string{ticketA, ticketB}, testXrefService.requestedTickets)

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a defn"}},
	}, rsp.GetExtendedXrefsReply().GetDefinitions())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "b defn"}},
	}, rsp.GetExtendedXrefsReply().GetGeneratedBy())

	assert.ElementsMatch(t, []*xrefpb.CrossReferencesReply_RelatedAnchor{
		{Anchor: &xrefpb.Anchor{Text: "a ref"}},
	}, rsp.GetExtendedXrefsReply().GetReferences())

	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverrides())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetOverriddenBy())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtends())
	assert.Empty(t, rsp.GetExtendedXrefsReply().GetExtendedBy())
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

// navLogPkg declares Print in the util/log package; navMain references it
// across packages (log.Print) and references a same-package func (greet).
const navLogPkg = `package log

func Print() {}
`

const navMain = `package main

import "github.com/example/repo/util/log"

func main() {
	log.Print()
	greet()
}

func greet() {}
`

func TestTreeSitterNav(t *testing.T) {
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
				{Filepath: "util/log/log.go", Content: []byte(navLogPkg)},
				{Filepath: "app/main.go", Content: []byte(navMain)},
			},
		}}},
	})
	require.NoError(t, err)

	// Decorations for app/main.go: both the cross-package selector
	// (log.Print, line 6) and the same-package reference (greet(), line 7)
	// are clickable. The frontend mints a tree-sitter:// file ticket; the
	// returned target tickets are opaque and carry the scheme forward.
	dec, err := server.tsNavDecorations(ctx, &xrefpb.DecorationsRequest{
		Location: &xrefpb.Location{Ticket: "tree-sitter://buildbuddy?path=app/main.go"},
	})
	require.NoError(t, err)
	refByLine := map[int32]*xrefpb.DecorationsReply_Reference{}
	for _, r := range dec.GetReference() {
		assert.Equal(t, "/kythe/edge/ref", r.GetKind())
		refByLine[r.GetSpan().GetStart().GetLineNumber()] = r
	}
	require.Contains(t, refByLine, int32(6), "cross-package selector log.Print should be decorated")
	require.Contains(t, refByLine, int32(7), "same-package reference greet() should be decorated")

	// Clicking the cross-package selector resolves to util/log/log.go:3.
	resolve := func(ticket string) *xrefpb.Anchor {
		t.Helper()
		xr, err := server.tsNavCrossReferences(ctx, &xrefpb.CrossReferencesRequest{Ticket: []string{ticket}})
		require.NoError(t, err)
		set := xr.GetCrossReferences()[ticket]
		require.NotNil(t, set, "no definition for %s", ticket)
		require.Len(t, set.GetDefinition(), 1)
		return set.GetDefinition()[0].GetAnchor()
	}

	logDef := resolve(refByLine[6].GetTargetTicket())
	assert.Equal(t, "tree-sitter://buildbuddy?path=util/log/log.go", logDef.GetParent())
	assert.Equal(t, int32(3), logDef.GetSpan().GetStart().GetLineNumber())

	// And the same-package reference resolves to greet's declaration (main.go:10).
	greetDef := resolve(refByLine[7].GetTargetTicket())
	assert.Equal(t, "tree-sitter://buildbuddy?path=app/main.go", greetDef.GetParent())
	assert.Equal(t, int32(10), greetDef.GetSpan().GetStart().GetLineNumber())

	// A ticket codenav didn't mint (a real kythe ticket) resolves to nothing.
	xr, err := server.tsNavCrossReferences(ctx, &xrefpb.CrossReferencesRequest{
		Ticket: []string{"kythe://buildbuddy?path=app/main.go"},
	})
	require.NoError(t, err)
	assert.Empty(t, xr.GetCrossReferences())

	// Hover (documentation) on the log.Print ticket: kind facts + definition.
	logTicket := refByLine[6].GetTargetTicket()
	docs, err := server.tsNavDocumentation(ctx, &spb.ExtendedDocumentationRequest{Ticket: logTicket})
	require.NoError(t, err)
	assert.Equal(t, "function", string(docs.GetNodeInfo().GetFacts()["/kythe/node/kind"]))
	assert.Equal(t, "tree-sitter://buildbuddy?path=util/log/log.go", docs.GetDefinition().GetAnchor().GetParent())
	assert.Equal(t, "func Print()", docs.GetDefinition().GetAnchor().GetSnippet())

	// References panel on log.Print: definition in util/log/log.go, and the
	// use site in app/main.go (line 6).
	xrefs, err := server.tsNavExtendedXrefs(ctx, &spb.ExtendedXrefsRequest{Tickets: []string{logTicket}})
	require.NoError(t, err)
	require.Len(t, xrefs.GetDefinitions(), 1)
	assert.Equal(t, "tree-sitter://buildbuddy?path=util/log/log.go", xrefs.GetDefinitions()[0].GetAnchor().GetParent())

	refLines := map[string]int32{}
	for _, ra := range xrefs.GetReferences() {
		path := ra.GetAnchor().GetParent()
		refLines[path] = ra.GetAnchor().GetSpan().GetStart().GetLineNumber()
	}
	assert.Equal(t, int32(6), refLines["tree-sitter://buildbuddy?path=app/main.go"],
		"the log.Print use in main.go should be a reference")
	assert.Empty(t, xrefs.GetOverrides())
	assert.Empty(t, xrefs.GetGeneratedBy())
}

// navTSLog declares Print in src/util/log.ts; navTSApp imports it (aliased +
// namespace) and references a same-module function.
const navTSLog = `// Print writes the default greeting.
export function Print(): void {}
`

const navTSApp = `import { Print as P } from "./util/log";
import * as log from "./util/log";

export function greet(): void {
  P();
  log.Print();
  helper();
}

function helper(): void {}
`

func TestTreeSitterNavTypeScript(t *testing.T) {
	ctx := t.Context()
	server := mustMakeServer(t)
	repo := "github.com/buildbuddy-io/buildbuddy"
	// TS identity is path-based; no module path needed.
	seedRepoMetadata(t, ctx, server, repo, "old", "")

	_, err := server.Index(ctx, &inpb.IndexRequest{
		GitRepo:             &gitpb.GitRepo{RepoUrl: repo},
		ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
		Update: &inpb.IncrementalUpdate{Commits: []*inpb.Commit{{
			Sha: "new", ParentSha: "old",
			AddsAndUpdates: []*inpb.File{
				{Filepath: "src/util/log.ts", Content: []byte(navTSLog)},
				{Filepath: "src/app.ts", Content: []byte(navTSApp)},
			},
		}}},
	})
	require.NoError(t, err)

	// Decorations for src/app.ts: the aliased import P (line 5), the namespace
	// member log.Print (line 6), and the same-module helper (line 7).
	dec, err := server.tsNavDecorations(ctx, &xrefpb.DecorationsRequest{
		Location: &xrefpb.Location{Ticket: "tree-sitter://buildbuddy?path=src/app.ts"},
	})
	require.NoError(t, err)
	refByLine := map[int32]*xrefpb.DecorationsReply_Reference{}
	for _, r := range dec.GetReference() {
		refByLine[r.GetSpan().GetStart().GetLineNumber()] = r
	}
	require.Contains(t, refByLine, int32(5), "aliased import P() should be decorated")
	require.Contains(t, refByLine, int32(6), "namespace member log.Print should be decorated")
	require.Contains(t, refByLine, int32(7), "same-module helper() should be decorated")

	// The aliased P and the namespace log.Print resolve to the same symbol.
	logTicket := "tree-sitter://symbol?pkg=ts%3Asrc%2Futil%2Flog&sym=Print"
	assert.Equal(t, logTicket, refByLine[5].GetTargetTicket())
	assert.Equal(t, logTicket, refByLine[6].GetTargetTicket())

	// Clicking it resolves to the declaration in src/util/log.ts:2.
	xr, err := server.tsNavCrossReferences(ctx, &xrefpb.CrossReferencesRequest{Ticket: []string{logTicket}})
	require.NoError(t, err)
	set := xr.GetCrossReferences()[logTicket]
	require.NotNil(t, set)
	require.Len(t, set.GetDefinition(), 1)
	assert.Equal(t, "tree-sitter://buildbuddy?path=src/util/log.ts", set.GetDefinition()[0].GetAnchor().GetParent())
	assert.Equal(t, int32(2), set.GetDefinition()[0].GetAnchor().GetSpan().GetStart().GetLineNumber())

	// Hover shows the function signature + doc.
	docs, err := server.tsNavDocumentation(ctx, &spb.ExtendedDocumentationRequest{Ticket: logTicket})
	require.NoError(t, err)
	assert.Equal(t, "function", string(docs.GetNodeInfo().GetFacts()["/kythe/node/kind"]))
	assert.Equal(t, "function Print(): void", docs.GetDefinition().GetAnchor().GetSnippet())
	assert.Equal(t, "Print writes the default greeting.", docs.GetDocstring())

	// References panel: both uses in app.ts (lines 5 and 6).
	xrefs, err := server.tsNavExtendedXrefs(ctx, &spb.ExtendedXrefsRequest{Tickets: []string{logTicket}})
	require.NoError(t, err)
	require.Len(t, xrefs.GetDefinitions(), 1)
	refLines := map[int32]bool{}
	for _, ra := range xrefs.GetReferences() {
		assert.Equal(t, "tree-sitter://buildbuddy?path=src/app.ts", ra.GetAnchor().GetParent())
		refLines[ra.GetAnchor().GetSpan().GetStart().GetLineNumber()] = true
	}
	assert.True(t, refLines[5] && refLines[6], "both P() and log.Print() uses should be references; got %v", refLines)
}

func TestNavRoutingByScheme(t *testing.T) {
	tsDec := &xrefpb.DecorationsRequest{Location: &xrefpb.Location{Ticket: "tree-sitter://buildbuddy?path=a.go"}}
	kytheDec := &xrefpb.DecorationsRequest{Location: &xrefpb.Location{Ticket: "kythe://buildbuddy?path=a.go"}}
	assert.True(t, decorationsUsesTreeSitter(tsDec))
	assert.False(t, decorationsUsesTreeSitter(kytheDec))

	tsXref := &xrefpb.CrossReferencesRequest{Ticket: []string{"tree-sitter://symbol?pkg=go%3Ax&sym=Y"}}
	kytheXref := &xrefpb.CrossReferencesRequest{Ticket: []string{"kythe://test?path=a"}}
	assert.True(t, crossReferencesUsesTreeSitter(tsXref))
	assert.False(t, crossReferencesUsesTreeSitter(kytheXref))

	tsDocs := &spb.ExtendedDocumentationRequest{Ticket: "tree-sitter://symbol?pkg=go%3Ax&sym=Y"}
	kytheDocs := &spb.ExtendedDocumentationRequest{Ticket: "kythe://test?path=a"}
	assert.True(t, docsUsesTreeSitter(tsDocs))
	assert.False(t, docsUsesTreeSitter(kytheDocs))

	tsExt := &spb.ExtendedXrefsRequest{Tickets: []string{"tree-sitter://symbol?pkg=go%3Ax&sym=Y"}}
	kytheExt := &spb.ExtendedXrefsRequest{Tickets: []string{"kythe://test?path=a"}}
	assert.True(t, extendedXrefsUsesTreeSitter(tsExt))
	assert.False(t, extendedXrefsUsesTreeSitter(kytheExt))
}
