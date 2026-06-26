package explain

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/flaghistory"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"
)

const timingProfileUsage = `
usage: bb explain timing-profile [--target API_TARGET] INVOCATION_ID

Analyzes the timing profile for the given invocation.
`

func handleTimingProfile(args []string) (int, error) {
	flags := flag.NewFlagSet("timing-profile", flag.ContinueOnError)
	flags.StringVar(apiTarget, "target", *apiTarget, "The API target to use for fetching the timing profile instead of the last --bes_backend.")
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			log.Printf("Failed to parse flags: %s", err)
		}
		log.Print(timingProfileUsage)
		return 1, nil
	}
	if len(flags.Args()) != 1 {
		log.Print(timingProfileUsage)
		return 1, nil
	}
	invocationID := flags.Args()[0]
	return analyzeTimingProfile(invocationID)
}

func analyzeTimingProfile(invocationIDOrURL string) (int, error) {
	invocationID := invocationIDOrURL
	if matches := uuidPattern.FindStringSubmatch(invocationIDOrURL); matches != nil {
		invocationID = matches[1]
	}

	profile, invocationDurationUsec, err := openTimingProfile(invocationID)
	if err != nil {
		return -1, err
	}
	defer profile.Close()

	report, err := AnalyzeTimingProfile(profile)
	if err != nil {
		return -1, fmt.Errorf("analyze timing profile: %w", err)
	}
	report.InvocationID = invocationID
	report.InvocationDurationUsec = invocationDurationUsec
	WriteTimingProfileReport(os.Stdout, report)
	return 0, nil
}

func openTimingProfile(invocationID string) (io.ReadCloser, int64, error) {
	apiKey, err := login.GetAPIKey()
	if err != nil {
		return nil, 0, err
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-buildbuddy-api-key", apiKey)
	backend := *apiTarget
	if backend == "" {
		backend, err = flaghistory.GetLastBackend()
		if err != nil {
			log.Debugf("Failed to get last backend: %v", err)
		}
		if backend == "" {
			backend = login.DefaultApiTarget
		}
	}
	conn, err := grpc_client.DialSimple(backend)
	if err != nil {
		return nil, 0, err
	}
	resource, invocationDurationUsec, err := getTimingProfileResource(ctx, conn, invocationID)
	if err != nil {
		conn.Close()
		return nil, 0, err
	}

	bsClient := bspb.NewByteStreamClient(conn)
	in, out := io.Pipe()
	go func() {
		defer conn.Close()
		err := cachetools.GetBlob(ctx, bsClient, resource, out)
		if err != nil {
			out.CloseWithError(fmt.Errorf("failed to download timing profile %s for invocation %s: %v", resource.DownloadString(), invocationID, err))
		} else {
			out.Close()
		}
	}()
	return in, invocationDurationUsec, nil
}

func getTimingProfileResource(ctx context.Context, conn *grpc_client.ClientConnPool, invocationID string) (*digest.CASResourceName, int64, error) {
	resp, err := bbspb.NewBuildBuddyServiceClient(conn).GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{InvocationId: invocationID},
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch invocation %s: %v", invocationID, err)
	}
	if len(resp.GetInvocation()) == 0 {
		return nil, 0, fmt.Errorf("no such invocation: %s", invocationID)
	}
	inv := resp.GetInvocation()[0]
	file := findTimingProfileLog(inv)
	if file == nil {
		return nil, 0, fmt.Errorf("no timing profile found for invocation %s", invocationID)
	}
	if !strings.HasPrefix(file.GetUri(), "bytestream://") {
		return nil, 0, fmt.Errorf("unsupported timing profile URI for %q: %s", file.GetName(), file.GetUri())
	}
	bytestreamURL, err := url.Parse(file.GetUri())
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse timing profile URI: %v", err)
	}
	resource, err := digest.ParseDownloadResourceName(bytestreamURL.Path)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse timing profile resource: %v", err)
	}
	return resource, inv.GetDurationUsec(), nil
}

func findTimingProfileLog(inv *inpb.Invocation) *bespb.File {
	var logs []*bespb.File
	for _, event := range inv.GetEvent() {
		logs = append(logs, event.GetBuildEvent().GetBuildToolLogs().GetLog()...)
	}

	if bazelMajorVersion(inv) >= 8 {
		if file := findCommandProfileLog(logs); file != nil {
			return file
		}
	}

	profileName := "command.profile.gz"
	if profilePath := getProfilePathFromCommandLine(inv.GetStructuredCommandLine()); profilePath != "" {
		profileName = filepath.Base(strings.ReplaceAll(profilePath, "\\", "/"))
	}
	for _, logFile := range logs {
		if logFile.GetUri() != "" && logFile.GetName() == profileName {
			return logFile
		}
	}
	return findCommandProfileLog(logs)
}

func findCommandProfileLog(logs []*bespb.File) *bespb.File {
	for _, logFile := range logs {
		if logFile.GetUri() != "" && strings.HasPrefix(logFile.GetName(), "command.profile.") {
			return logFile
		}
	}
	return nil
}

func getProfilePathFromCommandLine(commandLines []*clpb.CommandLine) string {
	for _, commandLine := range commandLines {
		if commandLine.GetCommandLineLabel() != "canonical" {
			continue
		}
		for _, section := range commandLine.GetSections() {
			if section.GetSectionLabel() != "command options" {
				continue
			}
			for _, option := range section.GetOptionList().GetOption() {
				if option.GetOptionName() == "profile" {
					return option.GetOptionValue()
				}
			}
		}
	}
	return ""
}

func bazelMajorVersion(inv *inpb.Invocation) int {
	for _, event := range inv.GetEvent() {
		version := event.GetBuildEvent().GetStarted().GetBuildToolVersion()
		if version == "" {
			continue
		}
		end := 0
		for end < len(version) && version[end] >= '0' && version[end] <= '9' {
			end++
		}
		if end == 0 {
			continue
		}
		major, err := strconv.Atoi(version[:end])
		if err == nil {
			return major
		}
	}
	return 0
}
