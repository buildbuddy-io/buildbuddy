package splash

import (
	"net"
	"net/url"
	"strconv"

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

	lines := []string{
		aurora.Sprintf(aurora.Reset("")),
		aurora.Sprintf(aurora.Reset("Your BuildBuddy server is running!").Bold()),
		aurora.Sprintf(aurora.Reset("")),
		aurora.Sprintf(aurora.Reset("Add the following lines to your %s to start sending build events to your local server:"), aurora.Bold(".bazelrc")),
		aurora.Sprintf(aurora.Reset("    build --bes_results_url=%s").BrightGreen().Bold(), resultsURL.String()),
		aurora.Sprintf(aurora.Reset("    build --bes_backend=%s").BrightGreen().Bold(), grpcURL.String()),
		aurora.Sprintf(aurora.Reset("")),
		aurora.Sprintf(aurora.Reset("You can now view BuildBuddy in the browser.")),
		aurora.Sprintf(aurora.Reset("    %s").Blue().Bold(), viewURL.String()),
	}
	for _, l := range lines {
		log.Print(l)
	}
}
