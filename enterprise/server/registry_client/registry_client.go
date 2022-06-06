package main

import (
	"context"
	"flag"
	"os"
	"runtime"

	regpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	server       = flag.String("server", "", "Target server")
	image        = flag.String("image", "", "Target image")
	username     = flag.String("username", "", "Username")
	password     = flag.String("password", "", "password")
	passwordFile = flag.String("password_file", "", "password file")
)

func main() {
	flag.Parse()

	conn, err := grpc_client.DialTargetWithOptions(*server, false)
	if err != nil {
		log.Fatalf("could not dial server: %s", err)
	}

	client := regpb.NewRegistryClient(conn)

	password := *password
	if *passwordFile != "" {
		data, err := os.ReadFile(*passwordFile)
		if err != nil {
			log.Fatalf("could not read password file: %s", err)
		}
		password = string(data)
	}

	req := &regpb.GetOptimizedImageRequest{
		Image: *image,
		ImageCredentials: &regpb.Credentials{
			Username: *username,
			Password: password,
		},
		Platform: &regpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
		},
	}
	rsp, err := client.GetOptimizedImage(context.Background(), req)
	if err != nil {
		log.Fatalf("call failed: %s", err)
	}
	log.Infof("got response: %s", rsp)
}
