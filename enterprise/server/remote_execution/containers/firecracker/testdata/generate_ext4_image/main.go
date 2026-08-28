package main

import (
	"context"
	"flag"
	"log"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ociconv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
)

var (
	image  = flag.String("image", "", "OCI image reference to convert")
	output = flag.String("output", "", "Path to the generated ext4 image")
)

func main() {
	flag.Parse()
	if *image == "" {
		log.Fatal("--image is required")
	}
	if *output == "" {
		log.Fatal("--output is required")
	}

	resolver, err := oci.NewResolver(real_environment.NewBatchEnv())
	if err != nil {
		log.Fatalf("Create OCI resolver: %s", err)
	}
	if err := ociconv.ConvertContainerToExt4FS(context.Background(), resolver, filepath.Dir(*output), *image, oci.Credentials{}, false, *output); err != nil {
		log.Fatalf("Convert image: %s", err)
	}
}
