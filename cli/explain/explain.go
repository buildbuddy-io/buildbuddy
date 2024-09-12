package explain

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/explain/compactgraph"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
	"github.com/gogo/protobuf/proto"
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
	spawnDiffs, err := diff(args[0], args[1])
	if err != nil {
		return -1, err
	}
	for _, spawnDiff := range spawnDiffs {
		log.Print(proto.MarshalTextString(spawnDiff))
	}
	log.Print("elapsed time: ", time.Since(before))
	return 0, nil
}

func diff(aPath, bPath string) ([]*spawn_diff.SpawnDiff, error) {
	readsEG := errgroup.Group{}
	var a compactgraph.CompactGraph
	var aHashFunction string
	readsEG.Go(func() (err error) {
		a, aHashFunction, err = readGraph(aPath)
		return err
	})
	var b compactgraph.CompactGraph
	var bHashFunction string
	readsEG.Go(func() (err error) {
		b, bHashFunction, err = readGraph(bPath)
		return err
	})
	if err := readsEG.Wait(); err != nil {
		return nil, err
	}
	if aHashFunction != bHashFunction {
		return nil, fmt.Errorf("hash functions differ: %q vs %q", aHashFunction, bHashFunction)
	}
	return compactgraph.Compare(a, b), nil
}

func readGraph(path string) (compactgraph.CompactGraph, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	return compactgraph.ReadCompactLog(f)
}

func formatTransitiveEffectSuffix(mnemonicAndCount map[string]uint) string {
	if len(mnemonicAndCount) == 0 {
		return ""
	}

	type kv struct {
		Mnemonic string
		Count    uint
	}
	var sorted []kv
	for k, v := range mnemonicAndCount {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		this, that := sorted[i], sorted[j]
		if this.Count != that.Count {
			return this.Count > that.Count
		}
		return this.Mnemonic < that.Mnemonic
	})

	var parts []string
	for _, mc := range sorted {
		parts = append(parts, fmt.Sprintf("%d %s", mc.Count, mc.Mnemonic))
	}
	return fmt.Sprintf(" (transitively invalidated: %s)", strings.Join(parts, ", "))
}
