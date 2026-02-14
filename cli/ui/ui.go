package ui

import (
	"flag"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
)

var (
	flags = flag.NewFlagSet("ui", flag.ContinueOnError)

	apiTarget  = flags.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target")
	userFlag   = flags.String("user", "", "Filter by user")
	meFlag     = flags.Bool("me", false, "Filter to your own builds (current $USER)")
	repoFlag   = flags.String("repo", "", "Filter by repo URL")
	branchFlag = flags.String("branch", "", "Filter by branch name")

	usage = `
bb ` + flags.Name() + ` [--me] [--user=<name>] [--repo=<url>] [--branch=<name>] [--target=<host>]

Opens an interactive terminal UI for browsing BuildBuddy builds.

Examples:
  bb ui
  bb ui --me
  bb ui --user=alice
  bb ui --branch=main
`
)

func HandleUI(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	apiKey, err := login.GetAPIKey()
	if err != nil || apiKey == "" {
		return -1, fmt.Errorf("not logged in; run `bb login` first: %w", err)
	}

	conn, err := grpc_client.DialSimple(*apiTarget)
	if err != nil {
		return -1, fmt.Errorf("failed to connect to BuildBuddy: %w", err)
	}
	defer conn.Close()

	client := bbspb.NewBuildBuddyServiceClient(conn)

	api := &apiClient{
		client: client,
		apiKey: apiKey,
	}

	// Fetch user info to determine org selection
	userResp, err := client.GetUser(api.ctx(), &uspb.GetUserRequest{})
	if err != nil {
		return -1, fmt.Errorf("failed to get user info: %w", err)
	}

	userFilter := *userFlag
	if *meFlag {
		userFilter = os.Getenv("USER")
	}

	f := filters{
		user:   userFilter,
		repo:   *repoFlag,
		branch: *branchFlag,
	}

	groups := userResp.GetUserGroup()
	var model rootModel
	if len(groups) > 1 {
		model = newRootModelWithOrgPicker(api, f, groups, userResp.GetSelectedGroup().GetGroupId())
	} else {
		api.groupID = userResp.GetSelectedGroup().GetGroupId()
		model = newRootModelWithList(api, f)
	}

	p := tea.NewProgram(
		model,
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		return -1, fmt.Errorf("error running UI: %w", err)
	}

	return 0, nil
}
