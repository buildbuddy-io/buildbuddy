package why

import (
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
	a, err := readGraph(args[0])
	if err != nil {
		return -1, err
	}
	b, err := readGraph(args[1])
	if err != nil {
		return -1, err
	}
	diags := compactgraph.Compare(a, b)
	for _, diag := range diags {
		log.Print(diag)
	}
	log.Print("elapsed time: ", time.Since(before))
	return 0, nil
}

func readGraph(path string) (compactgraph.CompactGraph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return compactgraph.ReadCompactLog(f)
}
