package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	n           = flag.Int("n", 1000, "number of runs to run")
	dir         = flag.String("dir", "", "dir to do things in")
	auth        = flag.Bool("auth", false, "if true, set groupID")
	concurrency = flag.Int("concurrency", 10, "number of goroutines to run")
)

func main() {
	flag.Parse()
	ctx := context.TODO()
	if *auth {
		ctx = claims.AuthContext(ctx, &claims.Claims{GroupID: "GR12345"})
	}

	log.Printf("Running %d loops...", *n)

	f, err := os.CreateTemp(*dir, "example")
	if err != nil {
		log.Fatalf("oh shit: %s", err)
	}
	f.Write([]byte("THIS IS SOME GREAT TESTING ISNT IT"))
	f.Write([]byte("REFLINKS ARE THE SHIT"))
	f.Close()
	log.Printf("Wrote some shit to %q", f.Name())

	fc, err := filecache.NewFileCache(filepath.Join(*dir, "filecache"), 1e9, true)
	if err != nil {
		log.Fatalf("Err making filecache: %s", err)
	}	
	d, err := digest.ComputeForFile(f.Name(), repb.DigestFunction_BLAKE3)
	if err != nil {
		log.Fatalf("Error hashing file: %s", err)
	}
	node := &repb.FileNode{
		Name:   "sourceFile",
		Digest: d,
	}
	log.Printf("Node is: %+v", node)
	if err := fc.AddFile(ctx, node, f.Name()); err != nil {
		log.Fatalf("error adding file: %s", err)
	}

	eg := errgroup.Group{}
	eg.SetLimit(*concurrency)

	start := time.Now()
	for i := 0; i < *n; i++ {
		i := i
		eg.Go(func() error {
			if !fc.FastLinkFile(ctx, node, fmt.Sprintf("%s-%d", f.Name(), i)) {
				log.Fatalf("Error fastlinking")
			}
			return nil
		})
	}
	eg.Wait()
	log.Printf("Finished %d loops in %s", *n, time.Since(start))
}
