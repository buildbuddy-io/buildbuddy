package why

import (
	"context"
	"golang.org/x/sync/errgroup"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/why/compactgraph"
)

const (
	usage = `
usage: bb why PATH PATH
`
)

func HandleWhy(args []string) (int, error) {
	if len(args) != 2 {
		log.Print(usage)
		return 1, nil
	}
	before := time.Now()
	ctx := context.Background()
	diags, err := diff(ctx, args[0], args[1])
	if err != nil {
		return -1, err
	}
	for _, diag := range diags {
		log.Print(diag)
	}
	log.Print("elapsed time: ", time.Since(before))
	return 0, nil
}

func diff(ctx context.Context, aPath, bPath string) ([]string, error) {
	reads, ctx := errgroup.WithContext(ctx)
	var a compactgraph.CompactGraph
	var b compactgraph.CompactGraph
	reads.Go(func() (err error) {
		a, err = readGraph(aPath)
		return err
	})
	reads.Go(func() (err error) {
		b, err = readGraph(bPath)
		return err
	})
	if err := reads.Wait(); err != nil {
		return nil, err
	}
	return compactgraph.Compare(a, b), nil
}

func readGraph(path string) (compactgraph.CompactGraph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return compactgraph.ReadCompactLog(f)
}
