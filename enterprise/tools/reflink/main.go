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

	files := make([]*os.File, *concurrency)
	nodes := make([]*repb.FileNode, *concurrency)

	
	fc, err := filecache.NewFileCache(filepath.Join(*dir, "filecache"), 1e9, true)
	if err != nil {
		log.Fatalf("Err making filecache: %s", err)
	}	


	for c := range *concurrency {
		f, err := os.CreateTemp(*dir, fmt.Sprintf("example-%d", c))
		if err != nil {
			log.Fatalf("oh shit: %s", err)
		}
		f.Write([]byte(fmt.Sprintf("This is source file %d", c)))
		f.Close()
		log.Printf("Wrote test file %d (%q)", c, f.Name())
		files[c] = f

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
		nodes[c] = node
	}
	eg := errgroup.Group{}
	eg.SetLimit(*concurrency)

	start := time.Now()
	for i := 0; i < *n; i++ {
		i := i
		f := files[i%(*concurrency)]
		node := nodes[i%(*concurrency)]
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
