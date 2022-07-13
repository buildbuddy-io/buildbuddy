package splash

import (
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/logrusorgru/aurora"
)

type Printer struct{}

// PrintSplashScreen prints out a nice welcome message with getting started
// instructions.
func (p *Printer) PrintSplashScreen(hostname string, port, gRPCPort int) {
	grpcURL := &url.URL{Scheme: "grpc", Host: net.JoinHostPort(hostname, strconv.Itoa(gRPCPort))}
	viewURL := &url.URL{Scheme: "http", Host: net.JoinHostPort(hostname, strconv.Itoa(port)), Path: "/"}
	resultsURL := &url.URL{Scheme: "http", Host: net.JoinHostPort(hostname, strconv.Itoa(port)), Path: "/invocation/"}

	linesColored := make(map[bool][]string)
	for _, colored := range []bool{true, false} {
		au := aurora.NewAurora(colored)
		linesColored[colored] = []string{
			au.Sprintf(au.Reset("")),
			au.Sprintf(au.Reset("   Your %s is up and running!").Bold(), au.BrightWhite("BuildBuddy Enterprise Server").Bold().Underline()),
			au.Sprintf(au.Reset("")),
			au.Sprintf(au.Reset("   Need help? Email us anytime at support@buildbuddy.io")),
			au.Sprintf(au.Reset("   Thanks for using BuildBuddy!")),
			au.Sprintf(au.Reset("")),
			au.Sprintf(au.Reset("   Add the following lines to your %s to start sending build"), au.Bold(".bazelrc")),
			au.Sprintf(au.Reset("   events to your local server:")),
			au.Sprintf(au.Reset("        build --bes_results_url=%s").BrightGreen().Bold(), resultsURL.String()),
			au.Sprintf(au.Reset("        build --bes_backend=%s").BrightGreen().Bold(), grpcURL.String()),
			au.Sprintf(au.Reset("")),
			au.Sprintf(au.Reset("   You can now view Buildbuddy in the browser:")),
			au.Sprintf(au.Reset("        %s").Blue().Bold(), viewURL.String()),
			au.Sprintf(au.Reset("")),
		}
	}

	maxLineLength := 0
	for _, l := range linesColored[false] {
		maxLineLength = int(math.Max(float64(maxLineLength), float64(len(l))))
	}

	paddedLineLength := maxLineLength + 2

	log.Print("")
	log.Print("╔" + strings.Repeat("═", paddedLineLength) + "╗")
	for i := range linesColored[true] {
		l := fmt.Sprintf("%-"+strconv.Itoa(paddedLineLength)+"s", linesColored[false][i])
		l = linesColored[true][i] + l[len(linesColored[false][i]):]
		log.Print("║" + l + "║")
	}
	log.Print("╚" + strings.Repeat("═", paddedLineLength) + "╝")
	log.Print("")
}
