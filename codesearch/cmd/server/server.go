package main

import (
	"net"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	_ "github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	kythe_server "github.com/buildbuddy-io/buildbuddy/codesearch/kythe/server"
	cs_server "github.com/buildbuddy-io/buildbuddy/codesearch/server"
	csspb "github.com/buildbuddy-io/buildbuddy/proto/codesearch_service"
	ksspb "github.com/buildbuddy-io/buildbuddy/proto/kythe_service"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
)

var (
	serverType = flag.String("server_type", "codesearch-server", "The server type to match on health checks")

	listen       = flag.String("codesearch.listen", ":2633", "Address to listen on")
	csIndexDir   = flag.String("codesearch.index_dir", "", "Directory to store index in")
	csScratchDir = flag.String("codesearch.scratch_dir", "", "Directory to store temp files in")

	kytheIndexDir   = flag.String("codesearch.kythe.index_dir", "", "Directory to store index in")
	kytheScratchDir = flag.String("codesearch.kythe.scratch_dir", "", "Directory to store temp files in")
)

func main() {
	flag.Parse()

	if err := config.Load(); err == nil {
		config.ReloadOnSIGHUP()
	}

	log.Printf("csIndexDir %s, csScratchDir: %s", *csIndexDir, *csScratchDir)
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err.Error())
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)
	env.SetAuthenticator(nullauth.NewNullAuthenticator(true /*anonymousEnabled*/, ""))

	css, err := cs_server.New(*csIndexDir, *csScratchDir)
	if err != nil {
		log.Fatal(err.Error())
	}

	options := grpc_server.CommonGRPCServerOptions(env)
	server := grpc.NewServer(options...)
	reflection.Register(server)

	grpc_prometheus.Register(server)
	grpc_prometheus.EnableHandlingTimeHistogram()

	csspb.RegisterCodesearchServiceServer(server, css)

	// Register kythe, if it's enabled.
	if *kytheIndexDir != "" {
		kss, err := kythe_server.New(*kytheIndexDir)
		if err != nil {
			log.Fatal(err.Error())
		}
		ksspb.RegisterXRefServiceServer(server, kss.XrefsService())
		ksspb.RegisterGraphServiceServer(server, kss.GraphService())
		ksspb.RegisterFileTreeServiceServer(server, kss.FiletreeService())
		ksspb.RegisterIdentifierServiceServer(server, kss.IdentifierService())
	}

	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(server))
	go func() {
		_ = server.Serve(lis)
	}()
	log.Printf("Codesearch server listening at %v", lis.Addr())
	env.GetHealthChecker().WaitForGracefulShutdown()
}
