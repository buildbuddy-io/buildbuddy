package ask

import (
	"context"
	"flag"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	supb "github.com/buildbuddy-io/buildbuddy/proto/suggestion"
)

var (
	flags  = flag.NewFlagSet("ask", flag.ContinueOnError)
	openai = flags.Bool("openai", false, "If true, use openai endpoint")
)

const (
	invocationIDFlagName = "invocation_id"
	besBackendFlagName   = "bes_backend"
)

var (
	usage = `
usage: bb ` + flags.Name() + ` [--openai|-o]

Asks for suggestions about the previous invocation.
`
)

func HandleAsk(args []string) (int, error) {
	command, idx := arg.GetCommandAndIndex(args)
	if command != "ask" && command != "wtf" && command != "huh" {
		return -1, nil
	}

	if idx != 0 {
		log.Debugf("Unexpected flag: %s", args[0])
		return 1, nil
	}
	flags.BoolVar(openai, "o", *openai, "alias for --openai")
	if err := arg.ParseFlagSet(flags, args[idx+1:]); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return 1, err
	}

	lastIID, err := getPreviousFlag(invocationIDFlagName)
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

	apiKey, err := storage.ReadRepoConfig("api-key")
	if err != nil {
		exitCode, err := login.HandleLogin([]string{"login"})
		if exitCode > 0 || err != nil {
			return exitCode, err
		}
		apiKey, err = storage.ReadRepoConfig("api-key")
		if apiKey == "" || err != nil {
			return 1, err
		}
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-buildbuddy-api-key", apiKey)

	lastBackend, err := getPreviousFlag(besBackendFlagName)
	if lastBackend == "" || err != nil {
		log.Printf("The previous invocation didn't have the --bes_backend= set.")
		return 1, err
	}

	if !strings.HasPrefix(lastBackend, "grpc://") && !strings.HasPrefix(lastBackend, "grpcs://") {
		lastBackend = "grpcs://" + lastBackend
	}

	conn, err := grpc_client.DialTarget(lastBackend)
	if err != nil {
		return 1, err
	}
	client := bbspb.NewBuildBuddyServiceClient(conn)
	res, err := client.GetSuggestion(ctx, req)
	if err != nil {
		return 1, err
	}

	for _, s := range res.Suggestion {
		log.Printf(s)
	}

	return 0, nil
}

// TODO(siggisim): Move this out of the ask package if we want to save more flags.
func SaveFlags(args []string) []string {
	command := arg.GetCommand(args)
	if command == "build" || command == "test" || command == "query" {
		saveFlag(args, besBackendFlagName, uuid.New())
		args = saveFlag(args, invocationIDFlagName, uuid.New())
	}
	return args
}

func saveFlag(args []string, flag, backup string) []string {
	value := arg.Get(args, flag)
	if value == "" {
		value = backup
	}
	args = append(args, "--"+flag+"="+value)
	os.WriteFile(getPreviousFlagPath(flag), []byte(value), 0777)
	return args
}

func getPreviousFlagPath(flagName string) string {
	workspaceDir, err := workspace.Path()
	if err != nil {
		return ""
	}
	dir, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return dir + "/last_bb_" + flagName + "-" + hash.String(workspaceDir) + ".txt"
}

func getPreviousFlag(flag string) (string, error) {
	lastValue, err := os.ReadFile(getPreviousFlagPath(flag))
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	return string(lastValue), nil
}
