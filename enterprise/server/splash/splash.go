package splash

import (
	"log"

	"github.com/logrusorgru/aurora"
)

type Printer struct{}

func (p *Printer) PrintSplashScreen(port, gRPCPort int) {
	// Print out a nice welcome message with getting started instructions.
	log.Printf("")
	log.Printf("╔════════════════════════════════════════════════════════════════════╗")
	log.Printf("║                                                                    ║")
	log.Printf("║   \u001b[37;1m\u001b[1mYour %s\u001b[37;1m\u001b[1m is up and running!\u001b[0m             ║", aurora.Underline(aurora.BrightWhite("BuildBuddy Enterprise Server")))
	log.Printf("║                                                                    ║")
	log.Printf("║   Need help? Email us anytime at support@buildbuddy.io             ║")
	log.Printf("║   Thanks for using BuildBuddy!                                     ║")
	log.Printf("║                                                                    ║")
	log.Printf("║   Add the following lines to your \u001b[37;1m\u001b[1m.bazelrc\u001b[0m to start sending build  ║")
	log.Printf("║   events to your local server:                                     ║")
	log.Printf("║        \u001b[32;1mbuild --bes_results_url=http://localhost:%d/invocation/\u001b[0m   ║", port)
	log.Printf("║        \u001b[32;1mbuild --bes_backend=grpc://localhost:%d\u001b[0m                   ║", gRPCPort)
	log.Printf("║                                                                    ║")
	log.Printf("║   You can now view Buildbuddy in the browser:                      ║")
	log.Printf("║        \u001b[34;1mhttp://localhost:%d/\u001b[0m                                      ║", port)
	log.Printf("║                                                                    ║")
	log.Printf("╚════════════════════════════════════════════════════════════════════╝")
	log.Printf("")
}
