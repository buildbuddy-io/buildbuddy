package why

import (
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/why/compactgraph"
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

	_, err = compactgraph.ReadCompactLog(f)
	if err != nil {
		return err
	}
	//err = execgraph.VerifyCompleteness(g)
	//if err != nil {
	//	return err
	//}
	return nil
}
