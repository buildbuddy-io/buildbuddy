//go:build !linux

package networking

import "fmt"

func startPacketCapture(interfaceName string, observe func([]byte)) (packetCapture, error) {
	return nil, fmt.Errorf("packet observation is unsupported on this platform")
}
