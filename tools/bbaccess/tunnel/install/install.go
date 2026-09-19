package install

import "net"

// splitHostPort splits a "host:port" listen address, defaulting the host to
// 127.0.0.1 when only a port is given.
func splitHostPort(listen string) (host, port string, err error) {
	h, p, err := net.SplitHostPort(listen)
	if err != nil {
		return "", "", err
	}
	if h == "" {
		h = "127.0.0.1"
	}
	return h, p, nil
}
