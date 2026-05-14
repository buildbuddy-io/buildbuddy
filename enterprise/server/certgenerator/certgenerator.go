package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/crypto/ssh"

	cgpb "github.com/buildbuddy-io/buildbuddy/proto/certgenerator"
)

var (
	serverType     = flag.String("server_type", "certgenerator-server", "The server type to match on health checks")
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")

	tokenIssuer     = flag.String("certgenerator.token.issuer", "", "Issuer of the OIDC token against which it will be validated.")
	tokenClientID   = flag.String("certgenerator.token.client_id", "", "When verifying the OIDC token, only tokens for this client ID will be accepted.")
	domain          = flag.String("certgenerator.token.domain", "", "When verifying the OIDC token, only tokens for this domain will be accepted.")
	serviceAccounts = flag.Slice("certgenerator.token.service_accounts", []string{}, "List of allowed service accounts.")

	sshCertDuration   = flag.Duration("certgenerator.ssh.validity", 12*time.Hour, "How long the generated certificate will be valid.")
	sshCertPrincipals = flag.String("certgenerator.ssh.principals", "", "Comma separated list of principals to include in the generated certificate.")

	caKeyFile = flag.String("certgenerator.ca_key_file", "", "Path to a PEM encoded certificate authority key file used to issue temporary user certificates.")
	caKey     = flag.String("certgenerator.ca_key", "", "PEM encoded certificate authority key used to issue temporary user certificates.", flag.Secret)

	kubernetesClusters     = flag.Slice("certgenerator.kubernetes.clusters", []KubernetesCluster{}, "List of clusters for which certificates will be generated.")
	kubernetesCertDuration = flag.Duration("certgenerator.kubernetes.validity", 12*time.Hour, "How long the generated certificate will be valid.")
)

const (
	// Allow user to get an interactive prompt when using SSH.
	allowPty            = "permit-pty"
	allowPortForwarding = "permit-port-forwarding"
)

type KubernetesCluster struct {
	Name        string `yaml:"name"`
	Server      string `yaml:"server"`
	ServerCA    string `yaml:"server_ca"`
	ClientCA    string `yaml:"client_ca"`
	ClientCAKey string `yaml:"client_ca_key"`
}

type parsedKubernetesCluster struct {
	*KubernetesCluster
	parsedClientCA    *x509.Certificate
	parsedClientCAKey *ecdsa.PrivateKey
}

func parseCertificate(data []byte) (*x509.Certificate, error) {
	cd, _ := pem.Decode(data)
	if cd == nil {
		return nil, status.InvalidArgumentError("certificate did not contain valid PEM data")
	}
	cert, err := x509.ParseCertificate(cd.Bytes)
	if err != nil {
		return nil, status.UnknownErrorf("could not parse certificate: %s", err)
	}
	return cert, nil
}

func parseCertificateKey(data []byte) (*ecdsa.PrivateKey, error) {
	kd, _ := pem.Decode(data)
	if kd == nil {
		return nil, status.InvalidArgumentErrorf("certificate key did not contain valid PEM data")
	}
	key, err := x509.ParseECPrivateKey(kd.Bytes)
	if err != nil {
		return nil, status.UnknownErrorf("could not parse certificate key: %s", err)
	}
	return key, nil
}

func parseKubernetesClusterConfig(c *KubernetesCluster) (*parsedKubernetesCluster, error) {
	_, err := parseCertificate([]byte(c.ServerCA))
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not load server CA for cluster %q: %s", c.Name, err)
	}
	pca, err := parseCertificate([]byte(c.ClientCA))
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not load client CA for cluster %q: %s", c.Name, err)
	}
	pk, err := parseCertificateKey([]byte(c.ClientCAKey))
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not load client CA key for cluster %q: %s", c.Name, err)
	}
	return &parsedKubernetesCluster{
		KubernetesCluster: c,
		parsedClientCA:    pca,
		parsedClientCAKey: pk,
	}, nil
}

type generator struct {
	verifier *oidc.IDTokenVerifier

	sshSigner          ssh.Signer
	kubernetesClusters []*parsedKubernetesCluster
}

type claims struct {
	Subject             string `json:"sub"`
	Domain              string `json:"hd"`
	Email               string `json:"email"`
	EmailVerified       bool   `json:"email_verified"`
	AuthorizedPresenter string `json:"azp"`
}

func validateUser(c *claims) error {
	if c.Domain == "" && slices.Contains(*serviceAccounts, c.Email) && c.AuthorizedPresenter == c.Email {
		return nil
	}
	if c.Domain != *domain {
		return status.PermissionDeniedErrorf("invalid domain %q", c.Domain)
	}
	return nil
}

func (g *generator) generateSSHCerts(c *claims, req *cgpb.GenerateRequest, rsp *cgpb.GenerateResponse) error {
	pk, _, _, _, err := ssh.ParseAuthorizedKey([]byte(req.GetSshPublicKey()))
	if err != nil {
		return status.InvalidArgumentErrorf("could not parse public key: %s", err)
	}
	cert := ssh.Certificate{
		Key:             pk,
		CertType:        ssh.UserCert,
		KeyId:           c.Email,
		ValidPrincipals: strings.Split(*sshCertPrincipals, ","),
		ValidAfter:      uint64(time.Now().Unix()),
		ValidBefore:     uint64(time.Now().Add(*sshCertDuration).Unix()),
	}
	cert.Permissions.Extensions = map[string]string{allowPty: "", allowPortForwarding: ""}
	if err := cert.SignCert(rand.Reader, g.sshSigner); err != nil {
		return err
	}
	rsp.SshCert = string(ssh.MarshalAuthorizedKey(&cert))
	return nil
}

func (g *generator) generateKubernetesCerts(c *claims, req *cgpb.GenerateRequest, rsp *cgpb.GenerateResponse) error {
	for _, kc := range g.kubernetesClusters {
		caCert := &ssl.CACert{
			Cert: kc.parsedClientCA,
			Key:  kc.parsedClientCAKey,
		}
		subject := pkix.Name{
			Organization: []string{"system:masters"},
			CommonName:   "system:admin",
		}
		cert, key, err := ssl.GenerateCert(subject, caCert, *kubernetesCertDuration)
		if err != nil {
			return err
		}
		rsp.KubernetesCredentials = append(rsp.KubernetesCredentials, &cgpb.KubernetesClusterCredentials{
			Name:          kc.Name,
			Server:        kc.Server,
			ServerCa:      kc.ServerCA,
			ClientCa:      kc.ClientCA,
			ClientCert:    cert,
			ClientCertKey: key,
		})
	}
	return nil
}

func (g *generator) Generate(ctx context.Context, req *cgpb.GenerateRequest) (*cgpb.GenerateResponse, error) {
	idToken, err := g.verifier.Verify(ctx, req.GetToken())
	if err != nil {
		return nil, status.PermissionDeniedErrorf("invalid token: %s", err)
	}
	c := &claims{}
	if err := idToken.Claims(c); err != nil {
		return nil, status.PermissionDeniedErrorf("could not parse claims: %s", err)
	}

	log.Infof("Certificate request for: %+v", c)

	if !c.EmailVerified {
		return nil, status.PermissionDeniedError("email not verified")
	}
	if err := validateUser(c); err != nil {
		return nil, status.PermissionDeniedErrorf("invalid domain %q for user %q", c.Domain, c.Email)
	}

	log.Infof("Generating certificates for: %+v", c)

	rsp := &cgpb.GenerateResponse{}

	if req.GetSshPublicKey() != "" {
		if err := g.generateSSHCerts(c, req, rsp); err != nil {
			return nil, err
		}
	}

	if err := g.generateKubernetesCerts(c, req, rsp); err != nil {
		return nil, err
	}

	return rsp, nil
}

func newGenerator(ctx context.Context) (*generator, error) {
	provider, err := oidc.NewProvider(ctx, *tokenIssuer)
	if err != nil {
		return nil, status.UnavailableErrorf("could not create OIDC provider: %s", err)
	}
	key, err := ssl.LoadCertificateKey(*caKeyFile, *caKey)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("could not load CA certificate: %s", err)
	}
	s, err := ssh.NewSignerFromKey(key)
	if err != nil {
		return nil, err
	}

	var kcs []*parsedKubernetesCluster
	for _, kc := range *kubernetesClusters {
		pkc, err := parseKubernetesClusterConfig(&kc)
		if err != nil {
			return nil, err
		}
		kcs = append(kcs, pkc)
	}

	g := &generator{
		verifier:           provider.Verifier(&oidc.Config{ClientID: *tokenClientID}),
		sshSigner:          s,
		kubernetesClusters: kcs,
	}
	return g, nil
}

func main() {
	flag.Parse()
	if err := configsecrets.Configure(); err != nil {
		log.Fatalf("Could not prepare config secrets provider: %s", err)
	}
	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}
	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}
	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)
	env.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))
	if err := auth.RegisterNullAuth(env); err != nil {
		log.Fatalf("Could not configure auth: %s", err)
	}

	if err := ssl.Register(env); err != nil {
		log.Fatalf("Could not enable SSL: %s", err)
	}

	ctx := context.Background()
	gen, err := newGenerator(ctx)
	if err != nil {
		log.Fatalf("Could not create generator: %s", err)
	}

	s, err := grpc_server.New(env, grpc_server.GRPCPort(), false /*=ssl*/, grpc_server.GRPCServerConfig{})
	if err != nil {
		log.Fatalf("Could not create gRPC server: %s", err)
	}
	cgpb.RegisterCertGeneratorServer(s.GetServer(), gen)
	if err := s.Start(); err != nil {
		log.Fatalf("Could not start gRPC server: %s", err)
	}

	if env.GetSSLService().IsEnabled() {
		s, err := grpc_server.New(env, grpc_server.GRPCSPort(), true /*=ssl*/, grpc_server.GRPCServerConfig{})
		if err != nil {
			log.Fatalf("Could not create gRPC server: %s", err)
		}
		cgpb.RegisterCertGeneratorServer(s.GetServer(), gen)
		if err := s.Start(); err != nil {
			log.Fatalf("Could not start gRPC server: %s", err)
		}
	}

	env.GetMux().Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	env.GetMux().Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: env.GetMux(),
	}

	go func() {
		server.ListenAndServe()
	}()

	monitoring.StartMonitoringHandler(env, fmt.Sprintf("%s:%d", *listen, *monitoringPort))

	healthChecker.WaitForGracefulShutdown()
}
