package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ociconv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

var (
	image  = flag.String("image", "", "OCI image reference to convert")
	arch   = flag.String("arch", "", "Architecture of the image to convert, either x86_64 or arm64")
	output = flag.String("output", "", "Path to the generated ext4 image")
)

func main() {
	flag.Parse()
	if err := log.Configure(); err != nil {
		log.Fatalf("configure logging: %s", err)
	}
	if err := run(context.Background()); err != nil {
		log.Fatalf("%s", err)
	}
}

func run(ctx context.Context) error {
	// Make sure we're running as root, otherwise the resulting image can end up
	// with incorrect file ownership/permissions.
	if os.Geteuid() != 0 {
		return fmt.Errorf("image conversion requires root; build with --config=remote")
	}
	if *image == "" {
		return fmt.Errorf("--image is required")
	}
	if !strings.Contains(*image, "@sha256:") {
		return fmt.Errorf("--image must use a pinned sha256 digest")
	}
	if *output == "" {
		return fmt.Errorf("--output is required")
	}
	platform := &rgpb.Platform{Os: "linux"}
	switch *arch {
	case "x86_64":
		platform.Arch = "amd64"
	case "arm64":
		platform.Arch = "arm64"
	default:
		return fmt.Errorf("--arch must be x86_64 or arm64")
	}

	resolver, err := oci.NewResolver(real_environment.NewBatchEnv())
	if err != nil {
		return fmt.Errorf("create OCI resolver: %w", err)
	}
	img, err := resolver.Resolve(ctx, *image, platform, oci.Credentials{}, false)
	if err != nil {
		return fmt.Errorf("resolve image for linux/%s: %w", platform.GetArch(), err)
	}
	config, err := img.ConfigFile()
	if err != nil {
		return fmt.Errorf("read image config: %w", err)
	}
	// Prevent an amd64 image from being published as arm64, or vice versa.
	// If the digest identifies a single-architecture image, Resolve returns it
	// even when its architecture differs from the requested platform.
	if config.OS != platform.GetOs() || config.Architecture != platform.GetArch() {
		return fmt.Errorf("image %q is %s/%s, want linux/%s", *image, config.OS, config.Architecture, platform.GetArch())
	}
	if err := ociconv.ConvertContainerToExt4FS(ctx, img, filepath.Dir(*output), *image, *output); err != nil {
		return fmt.Errorf("convert image: %w", err)
	}
	return nil
}
