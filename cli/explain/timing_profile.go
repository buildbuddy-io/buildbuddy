package explain

import (
	"errors"
	"flag"
	"io"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/util"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const timingProfileUsage = `
usage: bb explain timing-profile [--target API_TARGET] {INVOCATION_ID | INVOCATION_URL}

Examples:
  bb explain timing-profile 5e4e42d1-f545-4a21-8135-0e308d9f247a
  bb explain timing-profile https://app.buildbuddy.io/invocation/5e4e42d1-f545-4a21-8135-0e308d9f247a

Analyzes the timing profile for the given invocation.
`

var (
	timingProfileFlags  = flag.NewFlagSet("timing-profile", flag.ContinueOnError)
	timingProfileTarget = timingProfileFlags.String("target", login.DefaultApiTarget, "The API target to use for fetching the timing profile.")
)

func handleTimingProfile(args []string) (int, error) {
	if err := arg.ParseFlagSet(timingProfileFlags, args); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			log.Printf("Failed to parse flags: %s", err)
		}
		log.Print(timingProfileUsage)
		return 1, nil
	}
	if len(timingProfileFlags.Args()) != 1 {
		log.Print(timingProfileUsage)
		return 1, nil
	}
	invocationID := timingProfileFlags.Args()[0]
	return analyzeTimingProfile(invocationID)
}

func analyzeTimingProfile(invocationIDOrURL string) (int, error) {
	invocationID := invocationIDOrURL
	if matches := uuidPattern.FindStringSubmatch(invocationIDOrURL); matches != nil {
		invocationID = matches[1]
	}

	profile, err := openTimingProfile(invocationID)
	if err != nil {
		return -1, err
	}
	defer profile.Close()

	// TODO: Analyze profile.
	return 0, nil
}

func openTimingProfile(invocationID string) (io.ReadCloser, error) {
	return util.OpenInvocationFile(invocationID, *timingProfileTarget, "timing profile", findTimingProfileLog)
}

func findTimingProfileLog(inv *inpb.Invocation) *bespb.File {
	for _, event := range inv.GetEvent() {
		for _, logFile := range event.GetBuildEvent().GetBuildToolLogs().GetLog() {
			if logFile.GetUri() != "" && strings.HasPrefix(logFile.GetName(), "command.profile.") {
				return logFile
			}
		}
	}
	return nil
}
