package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/tools/bbcert/update"

	cgpb "github.com/buildbuddy-io/buildbuddy/proto/certgenerator"
)

var (
	servers = flag.Slice("server", []string{}, "gRPC target(s) for the certificate server(s). Can be specified multiple times. Defaults to the servers built into this binary, if any (see `bbcert version`).")
	// TODO: default to true once the publish workflow is in place.
	autoUpdate = flag.Bool("auto_update", false, "Check for a newer published bbcert before running, and switch to it. "+update.NoUpdateEnv+"=1 disables the check regardless.")
)

// defaultServers is a comma-separated server list stamped in at link time.
// This allows a binary to be published embedded with default servers.
var defaultServers string

// builtInServers parses a stamped server list. An unstamped build leaves the
// "{STABLE_...}" placeholder, which counts as no servers.
func builtInServers(stamped string) []string {
	if stamped == "" || strings.HasPrefix(stamped, "{") {
		return nil
	}
	var out []string
	for s := range strings.SplitSeq(stamped, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

var nonAlphanumeric = regexp.MustCompile(`[^a-zA-Z0-9]+`)

// serverToSuffix converts a server address to a filename suffix.
// e.g., "grpcs://foo.bar" -> "__foo_bar"
func serverToSuffix(server string) string {
	// Remove the scheme (grpcs://, grpc://, etc.)
	if idx := strings.Index(server, "://"); idx != -1 {
		server = server[idx+3:]
	}
	return "__" + nonAlphanumeric.ReplaceAllString(server, "_")
}

const (
	sshDir = ".ssh"

	defaultRSAPublicKeyFile   = "id_rsa.pub"
	defaultEDDSAPublicKeyFile = "id_ed25519.pub"
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
	command, args := splitCommand(os.Args[1:])
	os.Args = append(os.Args[:1], args...)

	switch command {
	case "":
		flag.Parse()
		if *autoUpdate {
			selfUpdate()
		}
		fetchCerts()
	case "update":
		os.Exit(update.Run(context.Background(), args))
	case "version":
		fmt.Println(update.Version())
		if s := builtInServers(defaultServers); len(s) > 0 {
			fmt.Printf("built-in servers: %s\n", strings.Join(s, ", "))
		}
		if u := update.BaseURL(); u != "" {
			fmt.Printf("updates from:     %s\n", u)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", command)
		os.Exit(1)
	}
}

// splitCommand peels the subcommand, which is positional and precedes the
// flags, off the arguments.
func splitCommand(argv []string) (command string, flags []string) {
	if len(argv) > 0 && !strings.HasPrefix(argv[0], "-") {
		command, argv = argv[0], argv[1:]
	}
	return command, slices.Clone(argv)
}

// selfUpdate replaces this binary with the latest published one and restarts
// with the same arguments.
func selfUpdate() {
	if os.Getenv(update.NoUpdateEnv) != "" || update.Commit() == "" {
		// Disabled, or a development build, which must not be replaced by
		// whatever happens to be published.
		return
	}
	ctx := context.Background()
	u, err := update.Default()
	if err != nil {
		log.Warningf("Not checking for updates: %s", err)
		return
	}
	m, needed, err := u.Check(ctx)
	if err != nil {
		log.Warningf("Could not check for updates: %s", err)
		return
	}
	if !needed {
		return
	}
	log.Infof("Updating bbcert %.12s -> %.12s (published %s)", update.Commit(), m.Commit, m.PublishedAt)
	if err := u.Apply(ctx, m); err != nil {
		log.Warningf("Could not update: %s", err)
		return
	}
	if err := update.Reexec(); err != nil {
		log.Warningf("Updated, but could not restart (%s); continuing with the previous version. Rerun bbcert to use the new one.", err)
	}
}

func fetchCerts() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Could not determine home directory: %s", err)
	}

	// Find the SSH key to use
	var keyFile string // e.g., "id_rsa"
	var pub []byte
	for _, kf := range []string{defaultRSAPublicKeyFile, defaultEDDSAPublicKeyFile} {
		publicKey := path.Join(homeDir, sshDir, kf)
		log.Infof("Trying public key %q.", publicKey)

		pub, err = os.ReadFile(publicKey)
		if err != nil {
			log.Infof("Could not read public %q key: %s", publicKey, err)
			continue
		}
		keyFile = strings.TrimSuffix(kf, ".pub")
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

	targets := *servers
	if len(targets) == 0 {
		targets = builtInServers(defaultServers)
		if len(targets) == 0 {
			log.Fatalf("No --server specified, and this build has no servers built in.")
		}
		log.Infof("Using the built-in servers: %s", strings.Join(targets, ", "))
	}

	for _, srv := range targets {
		if err := fetchCert(ctx, srv, homeDir, keyFile, pub, token); err != nil {
			log.Errorf("Failed to fetch cert from %s: %s", srv, err)
		}
	}
}

func fetchCert(ctx context.Context, server, homeDir, keyFile string, pub []byte, token string) error {
	log.Infof("Requesting certificate from %s.", server)

	conn, err := grpc_client.DialSimple(server)
	if err != nil {
		return fmt.Errorf("could not dial server: %s", err)
	}
	defer conn.Close()

	client := cgpb.NewCertGeneratorClient(conn)
	req := &cgpb.GenerateRequest{
		Token: token,
	}
	if pub != nil {
		req.SshPublicKey = string(pub)
	}
	resp, err := client.Generate(ctx, req)
	if err != nil {
		return fmt.Errorf("could not generate certificate: %s", err)
	}

	if resp.GetSshCert() != "" && keyFile != "" {
		suffix := serverToSuffix(server)
		certFile := keyFile + suffix + "-cert.pub"
		certPath := path.Join(homeDir, sshDir, certFile)

		// Create a copy of the private key with the suffixed name. SSH's naming
		// convention requires the cert file to be named <keyfile>-cert.pub.
		// Since we support multiple environments (e.g., dev and prod), each
		// with its own CA, we need multiple certs for the same key. This
		// requires separate key files (with identical content) so each can have
		// its own cert.
		srcKey := path.Join(homeDir, sshDir, keyFile)
		dstKey := path.Join(homeDir, sshDir, keyFile+suffix)
		if keyData, err := os.ReadFile(srcKey); err == nil {
			if err := os.WriteFile(dstKey, keyData, 0600); err != nil {
				log.Warningf("Could not copy key to %q: %s", dstKey, err)
			}
		}

		// ssh-agent caches certificates at add-time; renewing the cert on disk does
		// not update the agent. Remove the previous cert identity (if present)
		// before overwriting the cert file, so re-adding loads the renewed cert.
		if _, err := os.Stat(certPath); err == nil {
			_ = exec.CommandContext(ctx, "ssh-add", "-d", certPath).Run()
		}

		if err := os.WriteFile(certPath, []byte(resp.GetSshCert()), 0644); err != nil {
			return fmt.Errorf("could not write certificate to %q: %s", certPath, err)
		}
		log.Infof("Wrote certificate to %q.", certPath)

		// Add the key to the SSH agent. Since the key has a non-standard name,
		// SSH won't automatically find it.
		// See "identity_file" arg documentation in `man ssh`
		// TODO: maybe use an SSH config file with ProxyCommand instead?
		addCmd := exec.CommandContext(ctx, "ssh-add", dstKey)
		addCmd.Stderr = os.Stderr
		if err := addCmd.Run(); err != nil {
			log.Warningf("Could not add key to SSH agent: %s", err)
		} else {
			log.Infof("Added %q to SSH agent.", dstKey)
		}
	}

	for _, kc := range resp.KubernetesCredentials {
		log.Infof("Configuring credentials for %q kubernetes cluster.", "bb-"+kc.Name)
		if err := updateKubectlConfig(ctx, kc); err != nil {
			log.Warningf("Could not generate kubectl config for cluster %q: %s", kc.Name, err)
		}
	}

	return nil
}
