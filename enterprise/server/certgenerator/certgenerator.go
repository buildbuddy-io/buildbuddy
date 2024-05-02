package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"os"
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
	"github.com/coreos/go-oidc"
	"golang.org/x/crypto/ssh"

	cgpb "github.com/buildbuddy-io/buildbuddy/proto/certgenerator"
)

var (
	serverType     = flag.String("server_type", "certgenerator-server", "The server type to match on health checks")
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")

	tokenIssuer   = flag.String("certgenerator.token.issuer", "", "Issuer of the OIDC token against which it will be validated.")
	tokenClientID = flag.String("certgenerator.token.client_id", "", "When verifying the OIDC token, only tokens for this client ID will be accepted.")
	domain        = flag.String("certgenerator.token.domain", "", "When verifying the OIDC token, only tokens for this domain will be accepted.")

	sshCertDuration   = flag.Duration("certgenerator.ssh.validity", 12*time.Hour, "How long the generated certificate will be valid.")
	sshCertPrincipals = flag.String("certgenerator.ssh.principals", "", "Comma separated list of principals to include in the generated certificate.")

	caKeyFile = flag.String("certgenerator.ca_key_file", "", "Path to a PEM encoded certificate authority key file used to issue temporary user certificates.")
	caKey     = flag.String("certgenerator.ca_key", "", "PEM encoded certificate authority key used to issue temporary user certificates.", flag.Secret)
)

const (
	// Allow user to get an interactive prompt when using SSH.
	allowPty = "permit-pty"
)

type generator struct {
	verifier *oidc.IDTokenVerifier

	sshSigner ssh.Signer
}

func (g *generator) Generate(ctx context.Context, req *cgpb.GenerateRequest) (*cgpb.GenerateResponse, error) {
	idToken, err := g.verifier.Verify(ctx, req.GetToken())
	if err != nil {
		return nil, status.PermissionDeniedErrorf("invalid token: %s", err)
	}
	claims := struct {
		Subject       string `json:"sub"`
		Domain        string `json:"hd"`
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
	}{}
	if err := idToken.Claims(&claims); err != nil {
		return nil, status.PermissionDeniedErrorf("could not parse claims: %s", err)
	}
	if claims.Domain != *domain {
		return nil, status.PermissionDeniedErrorf("invalid domain %q", claims.Domain)
	}
	if !claims.EmailVerified {
		return nil, status.PermissionDeniedError("email not verified")
	}

	pk, _, _, _, err := ssh.ParseAuthorizedKey([]byte(req.GetSshPublicKey()))
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not parse public key: %s", err)
	}

	log.Infof("Generating certificate for: %+v", claims)

	cert := ssh.Certificate{
		Key:             pk,
		CertType:        ssh.UserCert,
		KeyId:           claims.Email,
		ValidPrincipals: strings.Split(*sshCertPrincipals, ","),
		ValidAfter:      uint64(time.Now().Unix()),
		ValidBefore:     uint64(time.Now().Add(*sshCertDuration).Unix()),
	}
	cert.Permissions.Extensions = map[string]string{allowPty: ""}
	if err := cert.SignCert(rand.Reader, g.sshSigner); err != nil {
		return nil, err
	}

	return &cgpb.GenerateResponse{
		SshCert: string(ssh.MarshalAuthorizedKey(&cert)),
	}, nil
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
	g := &generator{
		verifier:  provider.Verifier(&oidc.Config{ClientID: *tokenClientID}),
		sshSigner: s,
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
