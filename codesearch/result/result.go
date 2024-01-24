package result

import (
	"fmt"

	srpb "github.com/buildbuddy-io/buildbuddy/proto/codesearch/search"
)

type Result struct {
	Project  string
	Count    int
	Filename string
	Snippets [][]byte
}

func (r Result) String() string {
	out := fmt.Sprintf("%s [%d matches]\n", r.Filename, r.Count)
	for _, snip := range r.Snippets {
		out += fmt.Sprintf("  %s", string(snip))
	}
	return out
}

func (r Result) ToProto() *srpb.Result {
	p := &srpb.Result{
		Filename:   r.Filename,
		MatchCount: int32(r.Count),
		Repo:       r.Project,
	}
	for _, s := range r.Snippets {
		p.Snippets = append(p.Snippets, &srpb.Snippet{
			Lines: string(s),
		})
	}
	return p
}
