// mdutil contains utilities for working with gRPC metadata.
package mdutil

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

// Parse parses gRPC metadata from a list of strings in KEY=VALUE format.
func Parse(kvs ...string) (metadata.MD, error) {
	md := map[string][]string{}
	for _, s := range kvs {
		parts := strings.SplitN(s, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid KEY=VALUE pair")
		}
		md[parts[0]] = append(md[parts[0]], parts[1])
	}
	return md, nil
}
