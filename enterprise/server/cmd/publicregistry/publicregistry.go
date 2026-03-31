// Command publicregistry runs a standalone read-only OCI registry that
// mirrors images from gcr.io/flame-public under the /v2/buildbuddy/ path,
// with read-through caching via a remote BuildBuddy cache (AC + CAS).
//
// Usage:
//
//	publicregistry \
//	  --listen_addr=:8080 \
//	  --cache_backend=grpcs://remote.buildbuddy.dev \
//	  --cache_backend.api_key=YOUR_API_KEY
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	listenAddr   = flag.String("listen_addr", ":8080", "Address to listen on for HTTP traffic.")
	cacheBackend = flag.String("cache_backend", "", "gRPC target of the BuildBuddy cache (e.g. grpcs://remote.buildbuddy.dev).")
	apiKey       = flag.String("cache_backend.api_key", "", "API key for authenticating to the cache backend.")

	upstreamRegistry = flag.String("upstream_registry", "gcr.io", "Upstream OCI registry host.")
	upstreamProject  = flag.String("upstream_project", "flame-public", "Project/namespace in the upstream registry.")
	prefix           = flag.String("prefix", "buildbuddy", "Path prefix expected in /v2/<prefix>/<image>/... requests.")
)

func main() {
	flag.Parse()
	if err := log.Configure(); err != nil {
		fmt.Fprintf(os.Stderr, "Error configuring logging: %s\n", err)
		os.Exit(1)
	}

	if *cacheBackend == "" {
		log.Fatalf("--cache_backend is required")
	}

	healthChecker := healthcheck.NewHealthChecker("publicregistry")
	env := real_environment.NewRealEnv(healthChecker)

	// Connect to the remote cache.
	target := *cacheBackend
	if *apiKey != "" {
		target = addAPIKeyToTarget(target, *apiKey)
	}
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		log.Fatalf("Error dialing cache backend %q: %s", *cacheBackend, err)
	}
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetActionCacheClient(repb.NewActionCacheClient(conn))

	// Build the rewrite function:
	// "buildbuddy/executor" -> "gcr.io/flame-public/executor"
	expectedPrefix := *prefix + "/"
	reg := *upstreamRegistry
	proj := *upstreamProject
	rewrite := func(repository string) string {
		if strings.HasPrefix(repository, expectedPrefix) {
			imageName := strings.TrimPrefix(repository, expectedPrefix)
			return reg + "/" + proj + "/" + imageName
		}
		return repository
	}

	ocireg, err := ociregistry.New(env, ociregistry.Opts{
		RewriteRepository: rewrite,
		SimpleV2Check:     true,
		SkipAuthPrefix:    true,
	})
	if err != nil {
		log.Fatalf("Error creating OCI registry: %s", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/v2/", ocireg)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Error listening on %s: %s", *listenAddr, err)
	}
	log.Infof("Public registry listening on %s", lis.Addr())
	log.Infof("Mirroring %s/%s/* as /v2/%s/*/", reg, proj, *prefix)

	server := &http.Server{Handler: mux}
	go func() {
		<-healthChecker.Done()
		server.Shutdown(context.Background())
	}()
	if err := server.Serve(lis); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Error serving: %s", err)
	}
}

// addAPIKeyToTarget adds an API key as userinfo to a gRPC target URL.
// e.g. "grpcs://remote.buildbuddy.dev" -> "grpcs://MY_KEY@remote.buildbuddy.dev"
func addAPIKeyToTarget(target, key string) string {
	// grpc_client interprets userinfo as per-RPC credentials.
	if strings.Contains(target, "://") {
		parts := strings.SplitN(target, "://", 2)
		return parts[0] + "://" + key + "@" + parts[1]
	}
	return key + "@" + target
}
