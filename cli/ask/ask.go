package ask

import (
	"context"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	supb "github.com/buildbuddy-io/buildbuddy/proto/suggestion"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"google.golang.org/grpc/metadata"
)

var (
	flags  = flag.NewFlagSet("ask", flag.ContinueOnError)
	openai = flags.Bool("openai", false, "If true, use openai endpoint")
)

var (
	usage = `
usage: bb ` + flags.Name() + ` [--openai|-o]

Asks for suggestions about the previous invocation.
`
)

func HandleAsk(args []string) (int, error) {
	flags.BoolVar(openai, "o", *openai, "alias for --openai")
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return 1, err
	}

	lastIID, err := storage.GetPreviousFlag(storage.InvocationIDFlagName)
	if lastIID == "" || err != nil {
		log.Printf("Couldn't find the previous invocation.")
		return 1, err
	}

	req := &supb.GetSuggestionRequest{
		InvocationId: string(lastIID),
	}

	if *openai {
		req.Service = supb.SuggestionService_OPENAI
	}

	apiKey, err := login.GetAPIKeyInteractively()
	if err != nil {
		log.Warnf("Failed to enter login flow. Manually trigger with `bb login` .")
		return 1, err
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-buildbuddy-api-key", apiKey)

	backend, err := storage.GetLastBackend()
	if err != nil {
		return 1, err
	}
	conn, err := grpc_client.DialSimple(backend)
	if err != nil {
		return 1, err
	}
	client := bbspb.NewBuildBuddyServiceClient(conn)
	res, err := client.GetSuggestion(ctx, req)
	if err != nil {
		return 1, err
	}

	for _, s := range res.Suggestion {
		log.Print(s)
	}

	return 0, nil
}
