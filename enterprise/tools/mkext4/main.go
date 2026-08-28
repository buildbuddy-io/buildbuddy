// mkext4 builds an ext4 image from a directory using the native writer.
// Handy for experiments: mkext4 -dir /path -out img.ext4 -slack_mb 2000
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4writer"
)

var (
	dir         = flag.String("dir", "", "Source directory")
	out         = flag.String("out", "", "Output image path")
	slackMB     = flag.Int64("slack_mb", 2000, "Free space to leave")
	sizeMB      = flag.Int64("size_mb", 0, "Minimum image size")
	concurrency = flag.Int("concurrency", 0, "Copy workers")
	reflink     = flag.Bool("reflink", false, "Try FICLONERANGE")
	copyMode    = flag.String("copy_mode", "mmap", "mmap or cfr")
	xattrs      = flag.Bool("xattrs", false, "Copy extended attributes")
)

func main() {
	flag.Parse()
	start := time.Now()
	stats, err := ext4writer.DirectoryToImage(context.Background(), *dir, *out, &ext4writer.Options{SizeBytes: *sizeMB << 20, SlackBytes: *slackMB * 1e6, Concurrency: *concurrency, Reflink: *reflink, CopyMode: *copyMode, Xattrs: *xattrs})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Printf("%s total=%s\n", stats, time.Since(start))
}
