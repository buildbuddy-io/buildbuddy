package explain

import (
	"context"
	"errors"
	"flag"
	"io"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/util/download"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const profileUsage = `
usage: bb explain profile [--target API_TARGET] {INVOCATION_ID | INVOCATION_URL}

Examples:
  bb explain profile 5e4e42d1-f545-4a21-8135-0e308d9f247a
  bb explain profile https://app.buildbuddy.io/invocation/5e4e42d1-f545-4a21-8135-0e308d9f247a

Analyzes the timing profile for the given invocation.
`

var (
	profileFlags  = flag.NewFlagSet("profile", flag.ContinueOnError)
	profileTarget = profileFlags.String("target", login.DefaultApiTarget, "The API target to use for fetching the timing profile.")
)

func handleProfile(args []string) (int, error) {
	if err := arg.ParseFlagSet(profileFlags, args); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			log.Printf("Failed to parse flags: %s", err)
		}
		log.Print(profileUsage)
		return 1, nil
	}
	if len(profileFlags.Args()) != 1 {
		log.Print(profileUsage)
		return 1, nil
	}
	invocationID := profileFlags.Args()[0]
	return analyzeTimingProfile(invocationID)
}

func analyzeTimingProfile(invocationIDOrURL string) (int, error) {
	ctx := context.Background()

	invocationID := invocationIDOrURL
	if matches := uuidPattern.FindStringSubmatch(invocationIDOrURL); matches != nil {
		invocationID = matches[1]
	}

	profile, err := openTimingProfile(ctx, invocationID)
	if err != nil {
		return -1, err
	}
	defer profile.Close()

	// TODO: Analyze profile.
	return 0, nil
}

func openTimingProfile(ctx context.Context, invocationID string) (io.ReadCloser, error) {
	target, err := download.ResolveTarget(*profileTarget)
	if err != nil {
		return nil, err
	}
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		return nil, err
	}
	bsClient := bspb.NewByteStreamClient(conn)
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)

	// Avoid reading the entire profile into memory at once.
	in, out := io.Pipe()
	go func() {
		err := download.GetInvocationFile(ctx, bsClient, bbClient, out, invocationID, "timing profile", findTimingProfileLog)
		conn.Close()
		out.CloseWithError(err)
	}()
	return in, nil
}

func findTimingProfileLog(inv *inpb.Invocation) *bespb.File {
	// Starting in Bazel 8, the profile uploaded to the BEP may have a generated
	// name rather than the path specified by --profile. Match the prefix in this
	// case, consistent with the timing profile UI.
	majorVersion, ok := bazelMajorVersion(inv)
	if ok && majorVersion >= 8 {
		for _, event := range inv.GetEvent() {
			for _, logFile := range event.GetBuildEvent().GetBuildToolLogs().GetLog() {
				if logFile.GetUri() != "" && strings.HasPrefix(logFile.GetName(), "command.profile.") {
					return logFile
				}
			}
		}
		return nil
	}

	profileName := timingProfileName(inv)
	for _, event := range inv.GetEvent() {
		for _, logFile := range event.GetBuildEvent().GetBuildToolLogs().GetLog() {
			if logFile.GetUri() != "" && logFile.GetName() == profileName {
				return logFile
			}
		}
	}
	return nil
}

func timingProfileName(inv *inpb.Invocation) string {
	profilePath := "command.profile.gz"
	for _, commandLine := range inv.GetStructuredCommandLine() {
		if commandLine.GetCommandLineLabel() != "canonical" {
			continue
		}
		for _, section := range commandLine.GetSections() {
			if section.GetSectionLabel() != "command options" {
				continue
			}
			for _, option := range section.GetOptionList().GetOption() {
				if option.GetOptionName() == "profile" {
					profilePath = strings.ReplaceAll(option.GetOptionValue(), `\`, "/")
				}
			}
		}
	}
	if i := strings.LastIndex(profilePath, "/"); i >= 0 {
		return profilePath[i+1:]
	}
	return profilePath
}

func bazelMajorVersion(inv *inpb.Invocation) (int, bool) {
	for _, event := range inv.GetEvent() {
		version := event.GetBuildEvent().GetStarted().GetBuildToolVersion()
		segments := strings.Split(version, ".")
		if len(segments) < 2 {
			continue
		}
		major, err := strconv.Atoi(segments[0])
		if err != nil {
			continue
		}
		if _, err := strconv.Atoi(segments[1]); err != nil {
			continue
		}
		return major, true
	}
	return 0, false
}
