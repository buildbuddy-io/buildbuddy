package search

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/logrusorgru/aurora"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	srpb "github.com/buildbuddy-io/buildbuddy/proto/search"
)

var (
	flags = flag.NewFlagSet("search", flag.ContinueOnError)

	target     = flags.String("target", login.DefaultApiTarget, "Cache gRPC target")
	numResults = flags.Int("results", 10, "Print this many results")
	offset     = flags.Int("offset", 0, "Start printing results this far in")
	usage      = `
usage: bb ` + flags.Name() + ` query

Queries the codesearch index with the provided query.

Example of searching for a function:
  $ bb search func ResourceNameFromProto\(.*\)

Example of using different search atoms:
  $ bb search repo:buildbuddy-internal func lang:go
`
)

func searchCode(args []string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}

	ctx := context.Background()
	if apiKey, err := login.GetAPIKey(); err == nil && apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return err
	}

	client := bbspb.NewBuildBuddyServiceClient(conn)
	rsp, err := client.Search(ctx, &srpb.SearchRequest{
		Query: &srpb.Query{
			Term: strings.Join(args, " "),
		},
		NumResults: int32(*numResults),
		Offset:     int32(*offset),
	})
	if err != nil {
		return err
	}
	for i, res := range rsp.GetResults() {
		fmt.Println(aurora.Green(res.GetFilename()))
		for _, snip := range res.GetSnippets() {
			fmt.Print(snip.GetLines())
		}
		if i < len(rsp.GetResults())-1 {
			fmt.Print("\n")
		}
	}
	return nil
}

func HandleSearch(args []string) (int, error) {
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

	if err := searchCode(flags.Args()); err != nil {
		log.Print(err)
		return 1, nil
	}
	return 0, nil
}
