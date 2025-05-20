package index

import (
	"context"
	"errors"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/codesearch/github"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
)

var (
	flags = flag.NewFlagSet("index", flag.ContinueOnError)

	target = flags.String("target", login.DefaultApiTarget, "Codesearch gRPC target")
	// TODO(jdelfino): could maybe get this from `git remote get-url origin`, but who's to say
	// origin is defined?
	repoURL = flags.String("repo-url", login.DefaultApiTarget, "URL of the GitHub repo")

	usage = `
usage: bb ` + flags.Name() + `

Triggers an incremental update of the codesearch index.

All unindexed changes in the current repo will be submitted to the indexer for
asynchronous processing.
`
)

func indexRepo(args []string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}

	ctx := context.Background()
	if apiKey, err := storage.ReadRepoConfig("api-key"); err == nil && apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}

	parseRepoURL, err := git.ParseGitHubRepoURL(*repoURL)
	if err != nil {
		return err
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return err
	}

	client := bbspb.NewBuildBuddyServiceClient(conn)
	rsp, err := client.RepoStatus(ctx, &inpb.RepoStatusRequest{
		RepoUrl: parseRepoURL.String(),
	})
	if err != nil {
		return err
	}

	repoRoot, err := storage.RepoRootPath()
	if err != nil {
		return err
	}

	gc := github.NewCommandLineGitClient(repoRoot)

	headSHA, err := gc.ExecuteCommand("rev-parse", "HEAD")
	if err != nil {
		return err
	}

	update, err := github.ComputeIncrementalUpdate(gc, rsp.GetLastIndexedCommitSha(), headSHA)
	if err != nil {
		return err
	}

	_, err = client.Index(ctx, &inpb.IndexRequest{
		GitRepo: &gitpb.GitRepo{
			RepoUrl: parseRepoURL.String(),
			// TODO(jdelfino): shouldn't be required... reorg the proto?
			AccessToken: "",
			Username:    parseRepoURL.Owner,
		},
		ReplacementStrategy: inpb.ReplacementStrategy_INCREMENTAL,
		Update:              update,
		Async:               true,
	})

	if err != nil {
		return err
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

	if *repoURL == "" {
		log.Printf("A non-empty --repo-url must be specified")
		return 1, nil
	}

	if err := indexRepo(flags.Args()); err != nil {
		log.Print(err)
		return 1, nil
	}
	return 0, nil
}
