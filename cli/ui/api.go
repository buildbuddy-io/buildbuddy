package ui

import (
	"context"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

type apiClient struct {
	client  bbspb.BuildBuddyServiceClient
	apiKey  string
	groupID string
}

func (a *apiClient) ctx() context.Context {
	ctx := context.Background()
	if a.apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", a.apiKey)
	}
	return ctx
}

type filters struct {
	user   string
	repo   string
	branch string
}

// Messages
type invocationsMsg struct {
	invocations []*inpb.Invocation
	err         error
}

type invocationDetailMsg struct {
	invocation *inpb.Invocation
	err        error
}

type buildLogsMsg struct {
	logs string
	err  error
}

type tickMsg time.Time

// Commands

func (a *apiClient) fetchInvocations(f filters) tea.Cmd {
	return func() tea.Msg {
		sevenDaysAgo := time.Now().AddDate(0, 0, -7)
		query := &inpb.InvocationQuery{
			GroupId:      a.groupID,
			UpdatedAfter: timestamppb.New(sevenDaysAgo),
		}
		if f.user != "" {
			query.User = f.user
		}
		if f.repo != "" {
			query.RepoUrl = f.repo
		}
		if f.branch != "" {
			query.BranchName = f.branch
		}

		resp, err := a.client.SearchInvocation(a.ctx(), &inpb.SearchInvocationRequest{
			Query: query,
			Sort: &inpb.InvocationSort{
				SortField: inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD,
				Ascending: false,
			},
			Count: 50,
		})
		if err != nil {
			return invocationsMsg{err: err}
		}
		return invocationsMsg{invocations: resp.GetInvocation()}
	}
}

func (a *apiClient) fetchInvocationDetail(id string) tea.Cmd {
	return func() tea.Msg {
		resp, err := a.client.GetInvocation(a.ctx(), &inpb.GetInvocationRequest{
			Lookup: &inpb.InvocationLookup{
				InvocationId: id,
			},
		})
		if err != nil {
			return invocationDetailMsg{err: err}
		}
		invocations := resp.GetInvocation()
		if len(invocations) == 0 {
			return invocationDetailMsg{err: fmt.Errorf("invocation not found")}
		}
		return invocationDetailMsg{invocation: invocations[0]}
	}
}

func (a *apiClient) fetchBuildLogs(id string) tea.Cmd {
	return func() tea.Msg {
		resp, err := a.client.GetEventLogChunk(a.ctx(), &elpb.GetEventLogChunkRequest{
			InvocationId: id,
			MinLines:     1000,
		})
		if err != nil {
			return buildLogsMsg{err: err}
		}
		return buildLogsMsg{logs: string(resp.GetBuffer())}
	}
}

func autoRefreshCmd() tea.Cmd {
	return tea.Tick(10*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}
