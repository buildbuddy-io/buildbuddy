package why

import (
	"bufio"
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/why/execgraph"
)

const (
	usage = `
usage: bb why PATH...
`
)

func HandleWhy(args []string) (int, error) {
	if len(args) == 0 {
		log.Print(usage)
		return 1, nil
	}
	for _, path := range args {
		if err := printGraph(path); err != nil {
			return -1, err
		}
	}
	return 0, nil
}

func printGraph(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	g, err := execgraph.ReadGraph(reader)
	if err != nil {
		return err
	}
	roots := execgraph.FindRoots(g)
	for _, root := range roots {
		log.Print(root)
	}
	return nil
}
