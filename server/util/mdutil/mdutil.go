// package mdutil contains utilities for gRPC metadata.
package mdutil

import (
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"
)

// Parse parses a series of "NAME=VALUE" pairs into a metadata map.
func Parse(values []string) (metadata.MD, error) {
	var md metadata.MD
	for _, v := range values {
		pair := strings.SplitN(v, "=", 2)
		if len(pair) != 2 {
			return nil, status.InvalidArgumentErrorf(`malformed gRPC header %q (expected "NAME=VALUE")`, v)
		}
		md.Append(pair[0], pair[1])
	}
	return md, nil
}

// MakeSingleValued converts an MD to a single-valued mapping. If the original
// mapping contains multiple values for the same key, the last value wins.
func MakeSingleValued(md metadata.MD) map[string]string {
	out := make(map[string]string, len(md))
	for name, values := range md {
		if len(values) == 0 {
			out[name] = ""
		} else {
			out[name] = values[len(values)-1]
		}
	}
	return out
}
