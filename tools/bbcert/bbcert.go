package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"os/exec"
	"path"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	cgpb "github.com/buildbuddy-io/buildbuddy/proto/certgenerator"
)

var (
	server = flag.String("server", "", "gRPC target for the certificate server")
)

const (
	sshDir = ".ssh"

	defaultRSAPublicKeyFile   = "id_rsa.pub"
	defaultRSACertificateFile = "id_rsa-cert.pub"

	defaultEDDSAPublicKeyFile   = "id_ed25519.pub"
	defaultEDDSACertificateFile = "id_ed25519-cert.pub"
)

func kubectlConfigSet(ctx context.Context, key, value string) error {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := exec.CommandContext(ctx, "kubectl", "config", "set", key, value)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return status.UnknownErrorf("could not update kubectl configuration\n%s", stderr.String())
	}
	return nil
}

func updateKubectlConfig(ctx context.Context, kc *cgpb.KubernetesClusterCredentials) error {
	// Setup cluster config.
	clusterPrefix := "clusters.bb-" + kc.Name + "."
	if err := kubectlConfigSet(ctx, clusterPrefix+"server", kc.Server); err != nil {
		return err
	}
	if err := kubectlConfigSet(ctx, clusterPrefix+"certificate-authority-data", base64.StdEncoding.EncodeToString([]byte(kc.ServerCa))); err != nil {
		return err
	}

	// Setup user config.
	usersPrefix := "users.bb-" + kc.Name + "."
	if err := kubectlConfigSet(ctx, usersPrefix+"client-certificate-data", base64.StdEncoding.EncodeToString([]byte(kc.ClientCert))); err != nil {
		return err
	}
	if err := kubectlConfigSet(ctx, usersPrefix+"client-key-data", base64.StdEncoding.EncodeToString([]byte(kc.ClientCertKey))); err != nil {
		return err
	}

	// Create context.
	contextPrefix := "contexts.bb-" + kc.Name + "."
	if err := kubectlConfigSet(ctx, contextPrefix+"cluster", "bb-"+kc.Name); err != nil {
		return err
	}
	if err := kubectlConfigSet(ctx, contextPrefix+"user", "bb-"+kc.Name); err != nil {
		return err
	}
	return nil
}

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

	var certificateFile string
	var pub []byte
	for _, kf := range []string{defaultRSAPublicKeyFile, defaultEDDSAPublicKeyFile} {
		publicKey := path.Join(homeDir, sshDir, kf)
		log.Infof("Trying public key %q.", publicKey)

		pub, err = os.ReadFile(publicKey)
		if err != nil {
			log.Infof("Could not read public %q key: %s", publicKey, err)
			continue
		}
		if kf == defaultRSAPublicKeyFile {
			certificateFile = defaultRSACertificateFile
		} else {
			certificateFile = defaultEDDSACertificateFile
		}
		break
	}

	if pub == nil {
		log.Warningf("No ssh keys found, not requesting SSH certificate.")
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
	if pub != nil {
		req.SshPublicKey = string(pub)
	}
	resp, err := client.Generate(ctx, req)
	if err != nil {
		log.Fatalf("Could not generate certificate: %s", err)
	}

	if resp.GetSshCert() != "" {
		certFile := path.Join(homeDir, sshDir, certificateFile)
		if err := os.WriteFile(certFile, []byte(resp.GetSshCert()), 0644); err != nil {
			log.Fatalf("Could not write certificate to %q: %s", certFile, err)
		}
		log.Infof("Wrote certificate to %q.", certFile)
	}

	for _, kc := range resp.KubernetesCredentials {
		log.Infof("Configuring credentials for %q kubernetes cluster.", "bb-"+kc.Name)
		if err := updateKubectlConfig(ctx, kc); err != nil {
			log.Warningf("Could not generate kubectl config for cluster %q: %s", kc.Name, err)
		}
	}

}
