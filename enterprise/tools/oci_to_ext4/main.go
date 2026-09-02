// oci_to_ext4 converts an OCI container image to an ext4 root filesystem
// image. The app runs it as a remote action to build Firecracker root
// filesystem images once per image rather than on every executor.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
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
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "oci_to_ext4: %s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if *image == "" {
		return fmt.Errorf("--image is required")
	}
	if *output == "" {
		return fmt.Errorf("--output is required")
	}
	resolver, err := oci.NewResolver(real_environment.NewBatchEnv())
	if err != nil {
		return fmt.Errorf("create OCI resolver: %w", err)
	}
	creds := oci.Credentials{
		Username: os.Getenv("BUILDBUDDY_OCI_USERNAME"),
		Password: os.Getenv("BUILDBUDDY_OCI_PASSWORD"),
	}
	if err := ociconv.ConvertContainerToExt4FS(ctx, resolver, filepath.Dir(*output), *image, creds, false, *output); err != nil {
		return fmt.Errorf("convert image: %w", err)
	}
	return nil
}
