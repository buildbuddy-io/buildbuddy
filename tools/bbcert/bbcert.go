package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	cgpb "github.com/buildbuddy-io/buildbuddy/proto/certgenerator"
)

var (
	server = flag.String("server", "", "gRPC target for the certificate server")
)

const (
	sshDir = ".ssh"
	// For now, assume user is using an RSA key.
	defaultRSAPublicKeyFile   = "id_rsa.pub"
	defaultRSACertificateFile = "id_rsa-cert.pub"
)

func main() {
	flag.Parse()

	conn, err := grpc_client.DialSimple(*server)
	if err != nil {
		log.Fatalf("Could not dial server: %s", err)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Could not determine home directory: %s", err)
	}

	publicKey := path.Join(homeDir, sshDir, defaultRSAPublicKeyFile)
	log.Infof("Using public key %q.", publicKey)

	pub, err := os.ReadFile(publicKey)
	if err != nil {
		log.Fatalf("Could not read public %q key: %s", publicKey, err)
	}

	ctx := context.Background()

	log.Infof("Getting identity token from gcloud.")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := exec.CommandContext(ctx, "gcloud", "auth", "print-identity-token")
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("Could not get identity token from gcloud:\n%s", stderr.String())
	}
	token := stdout.String()

	log.Infof("Requesting certificate from remote server.")

	client := cgpb.NewCertGeneratorClient(conn)
	req := &cgpb.GenerateRequest{
		Token:        token,
		SshPublicKey: string(pub),
	}
	resp, err := client.Generate(ctx, req)
	if err != nil {
		log.Fatalf("Could not generate certificate: %s", err)
	}

	certFile := path.Join(homeDir, sshDir, defaultRSACertificateFile)
	if err := os.WriteFile(certFile, []byte(resp.GetSshCert()), 0644); err != nil {
		log.Fatalf("Could not write certificate to %q: %s", certFile, err)
	}

	log.Infof("Wrote certificate to %q.", certFile)
}
