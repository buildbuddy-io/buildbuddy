// This tool provides a mock SAML IDP server to test SAML auth locally.
// For usage, see README in this directory.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/mocksaml"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	certFile = flag.String("cert_file", "", "Path to PEM-encoded self-signed IDP cert.")
	keyFile  = flag.String("key_file", "", "Path to PEM-encoded self-signed IDP key file.")

	port          = flag.Int("port", 4000, "mock SAML server port")
	buildbuddyURL = flag.String("buildbuddy_url", "http://localhost:8080", "BuildBuddy server URL")
	slug          = flag.String("slug", "samltest", "BuildBuddy org slug")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx := context.Background()

	if *certFile == "" || *keyFile == "" {
		return fmt.Errorf("missing --cert_file or --key_file")
	}

	idp, err := startServer()
	if err != nil {
		return fmt.Errorf("init server: %w", err)
	}

	log.Infof("SAML IDP metadata URL: %s", idp.MetadataURL())
	log.Infof("SAML login URL: %s", idp.BuildBuddyLoginURL(*buildbuddyURL, *slug))

	go idp.CopyLogs(os.Stdout, os.Stderr)

	ch := make(chan os.Signal, 1)
	killed := make(chan struct{})
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		close(killed)
		log.Infof("Shutting down...")
		if err := idp.Kill(); err != nil {
			log.Errorf("Failed to kill server: %s", err)
		}
	}()
	err = idp.Wait(ctx)
	select {
	case <-killed:
		return nil
	default:
		return err
	}
}

func startServer() (*mocksaml.IDP, error) {
	cf, err := os.Open(*certFile)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", *certFile, err)
	}
	defer cf.Close()
	kf, err := os.Open(*keyFile)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", *keyFile, err)
	}
	defer kf.Close()
	return mocksaml.Start(*port, cf, kf)
}
