package network

import (
	"net"
	"strconv"
)

// ParseAddress parses an address like "localhost:1234" and returns the host as
// a string, port (int), and error.
func ParseAddress(addr string) (string, int, error) {
	host, portString, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.ParseUint(portString, 10, 16)
	if err != nil {
		return "", 0, err
	}
	return host, int(port), nil
}
