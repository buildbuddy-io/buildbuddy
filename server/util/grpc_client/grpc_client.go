package grpc_client

import (
	"net/url"

	"google.golang.org/grpc"
)

// New Client handles some of the logic around detecting the correct GRPC
// connection type and applying relevant options when connecting.
func DialTarget(target string) (*grpc.ClientConn, error) {
	dialOptions := make([]grpc.DialOption, 0)
	u, err := url.Parse(target)
	if err == nil {
		if u.User != nil {
			authStr := u.User.String() + "@" + u.Host
			dialOptions = append(dialOptions, grpc.WithAuthority(authStr))
		}
		if u.Scheme != "grpcs" {
			dialOptions = append(dialOptions, grpc.WithInsecure())
		}

		target = u.Host
	}

	// Connect to host/port and create a new client
	return grpc.Dial(target, dialOptions...)
}
