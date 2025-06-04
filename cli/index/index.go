package index

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/codesearch/github"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
)

var (
	flags = flag.NewFlagSet("index", flag.ContinueOnError)

	target  = flags.String("target", login.DefaultApiTarget, "Codesearch gRPC target")
	repoURL = flags.String("repo-url", "", "URL of the GitHub repo. Defaults to remote named 'origin' in the current repo.")

	usage = `
usage: bb ` + flags.Name() + `

Triggers an incremental update of the codesearch index.

All unindexed changes in the current repo will be submitted to the indexer for asynchronous processing.
`
)

func makeGitClient() (github.GitClient, error) {
	repoRoot, err := storage.RepoRootPath()
	if err != nil {
		return nil, err
	}
	log.Printf("Repo root: %s", repoRoot)

	return github.NewCommandLineGitClient(repoRoot), nil
}

func getRepoInfo(gc github.GitClient) (*git.RepoURL, string, error) {
	headSHA, err := gc.ExecuteCommand("rev-parse", "HEAD")
	if err != nil {
		return nil, "", err
	}
	headSHA = strings.TrimSpace(headSHA)

	var ru string
	if *repoURL == "" {
		result, err := gc.ExecuteCommand("remote", "get-url", "origin")
		if err != nil {
			return nil, "", fmt.Errorf("repo-url not provided, and could not get URL of 'origin' remote: %w", err)
		}
		ru = result
	} else {
		ru = *repoURL
	}

	parseRepoURL, err := git.ParseGitHubRepoURL(ru)
	if err != nil {
		return nil, "", err
	}

	return parseRepoURL, headSHA, nil
}

func buildIndexRequest(gc github.GitClient, repoURL *git.RepoURL, headSHA, lastIndexSHA string) (*inpb.IndexRequest, error) {
	req := &inpb.IndexRequest{
		GitRepo: &gitpb.GitRepo{
			RepoUrl:  repoURL.String(),
			Username: repoURL.Owner,
		},
	}

	if lastIndexSHA != "" {
		update, err := github.ComputeIncrementalUpdate(gc, lastIndexSHA, headSHA)
		if err != nil {
			if status.IsFailedPreconditionError(err) {
				log.Printf("Failed to compute incremental update, falling back to full re-index: %s", err)
			} else {
				return nil, fmt.Errorf("failed to compute incremental update: %w", err)
			}
		} else {
			req.ReplacementStrategy = inpb.ReplacementStrategy_INCREMENTAL
			req.Update = update
			return req, nil
		}
	} else {
		log.Printf("No previous index found for repo %s, performing full re-index.", repoURL)
	}

	req.ReplacementStrategy = inpb.ReplacementStrategy_REPLACE_REPO
	req.RepoState = &gitpb.RepoState{
		CommitSha: headSHA,
	}
	req.Async = true // Don't wait for full re-indexes to complete.
	// Access token will be added by the server based on user auth.
	return req, nil
}

func indexRepo() error {
	ctx := context.Background()

	gc, err := makeGitClient()
	if err != nil {
		return err
	}

	parsedRepoURL, headSHA, err := getRepoInfo(gc)
	if err != nil {
		return err
	}

	if apiKey, err := login.GetAPIKey(); err == nil && apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return err
	}

	client := bbspb.NewBuildBuddyServiceClient(conn)
	rsp, err := client.RepoStatus(ctx, &inpb.RepoStatusRequest{
		RepoUrl: parsedRepoURL.String(),
	})
	if err != nil {
		return fmt.Errorf("failed to get repo status: %w", err)
	}

	update, err := buildIndexRequest(gc, parsedRepoURL, headSHA, rsp.GetLastIndexedCommitSha())
	if err != nil {
		return fmt.Errorf("failed to create index request: %w", err)
	}

	_, err = client.Index(ctx, update)
	if err != nil {
		return err
	}

	if update.ReplacementStrategy == inpb.ReplacementStrategy_REPLACE_REPO {
		log.Printf("Re-indexing entire repo %s at commit %s", parsedRepoURL.String(), headSHA)
	} else {
		commits := update.GetUpdate().GetCommits()
		firstSha := commits[0].GetSha()
		lastSha := commits[len(commits)-1].GetSha()
		log.Printf("Completed incremental update for %s, %s..%s", parsedRepoURL.String(), firstSha, lastSha)
	}
	return nil
}

func HandleIndex(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return 1, err
	}

	if *target == "" {
		log.Printf("A non-empty --target must be specified")
		return 1, nil
	}

	if err := indexRepo(); err != nil {
		log.Print(err)
		return 1, nil
	}
	return 0, nil
}
