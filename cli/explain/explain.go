package explain

import (
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/explain/compactgraph"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"golang.org/x/sync/errgroup"
)

const (
	usage = `
usage: bb explain PATH PATH
`
)

func HandleExplain(args []string) (int, error) {
	if len(args) != 2 {
		log.Print(usage)
		return 1, nil
	}
	before := time.Now()
	diags, err := diff(args[0], args[1])
	if err != nil {
		return -1, err
	}
	for _, diag := range diags {
		log.Print(diag)
	}
	log.Print("elapsed time: ", time.Since(before))
	return 0, nil
}

func diff(aPath, bPath string) ([]string, error) {
	readsEG := errgroup.Group{}
	var a compactgraph.CompactGraph
	readsEG.Go(func() (err error) {
		a, err = readGraph(aPath)
		return err
	})
	var b compactgraph.CompactGraph
	readsEG.Go(func() (err error) {
		b, err = readGraph(bPath)
		return err
	})
	if err := readsEG.Wait(); err != nil {
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
