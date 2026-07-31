package networking

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"sort"
	"sync"
)

const (
	maxObservedDestinations = 1000
	maxObservedFlows        = 10_000
)

// NetworkDestination contains connection metadata observed for one remote
// network endpoint. It intentionally does not contain any packet payloads.
type NetworkDestination struct {
	IP              string `json:"ip"`
	Port            uint16 `json:"port"`
	Protocol        string `json:"protocol"`
	BytesSent       int64  `json:"bytes_sent"`
	BytesReceived   int64  `json:"bytes_received"`
	PacketsSent     int64  `json:"packets_sent"`
	PacketsReceived int64  `json:"packets_received"`
	ConnectionCount int64  `json:"connection_count"`
}

type destinationKey struct {
	ip       netip.Addr
	port     uint16
	protocol uint8
}

type flowKey struct {
	destinationKey
	localPort uint16
}

type packetMetadata struct {
	sourceIP, destinationIP     netip.Addr
	sourcePort, destinationPort uint16
	protocol                    uint8
	bytes                       int64
}

type packetCapture interface {
	Close() error
}

// PacketObserver passively collects per-destination network metadata from a
// runner's host-side virtual network interface.
type PacketObserver struct {
	runnerIP netip.Addr
	capture  packetCapture

	mu           sync.Mutex
	destinations map[destinationKey]*NetworkDestination
	flows        map[flowKey]struct{}
}

// NewPacketObserver starts observing packets on interfaceName. runnerIP is the
// IP assigned to the runner side of the host veth pair after namespace NAT.
func NewPacketObserver(interfaceName, runnerIP string) (*PacketObserver, error) {
	ip, err := netip.ParseAddr(runnerIP)
	if err != nil {
		return nil, fmt.Errorf("parse runner IP: %w", err)
	}
	o := newPacketObserver(ip)
	capture, err := startPacketCapture(interfaceName, o.observePacket)
	if err != nil {
		return nil, err
	}
	o.capture = capture
	return o, nil
}

func newPacketObserver(runnerIP netip.Addr) *PacketObserver {
	return &PacketObserver{
		runnerIP:     runnerIP,
		destinations: make(map[destinationKey]*NetworkDestination),
		flows:        make(map[flowKey]struct{}),
	}
}

// Close stops packet collection. It is safe to call more than once.
func (o *PacketObserver) Close() error {
	if o.capture == nil {
		return nil
	}
	return o.capture.Close()
}

// Reset clears all metadata collected so far.
func (o *PacketObserver) Reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.destinations = make(map[destinationKey]*NetworkDestination)
	o.flows = make(map[flowKey]struct{})
}

// Destinations returns a stable snapshot sorted by protocol, IP, and port.
func (o *PacketObserver) Destinations() []*NetworkDestination {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make([]*NetworkDestination, 0, len(o.destinations))
	for _, d := range o.destinations {
		clone := *d
		result = append(result, &clone)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Protocol != result[j].Protocol {
			return result[i].Protocol < result[j].Protocol
		}
		if result[i].IP != result[j].IP {
			return result[i].IP < result[j].IP
		}
		return result[i].Port < result[j].Port
	})
	return result
}

func (o *PacketObserver) observePacket(packet []byte) {
	md, ok := parsePacket(packet)
	if !ok {
		return
	}

	sent := md.sourceIP == o.runnerIP
	received := md.destinationIP == o.runnerIP
	if !sent && !received {
		return
	}

	remoteIP, remotePort, localPort := md.sourceIP, md.sourcePort, md.destinationPort
	if sent {
		remoteIP, remotePort, localPort = md.destinationIP, md.destinationPort, md.sourcePort
	}
	key := destinationKey{ip: remoteIP, port: remotePort, protocol: md.protocol}

	o.mu.Lock()
	defer o.mu.Unlock()
	destination, ok := o.destinations[key]
	if !ok {
		if len(o.destinations) >= maxObservedDestinations {
			return
		}
		destination = &NetworkDestination{
			IP:       remoteIP.String(),
			Port:     remotePort,
			Protocol: protocolName(md.protocol),
		}
		o.destinations[key] = destination
	}
	if sent {
		destination.BytesSent += md.bytes
		destination.PacketsSent++
	} else {
		destination.BytesReceived += md.bytes
		destination.PacketsReceived++
	}
	flow := flowKey{destinationKey: key, localPort: localPort}
	if _, ok := o.flows[flow]; !ok && len(o.flows) < maxObservedFlows {
		o.flows[flow] = struct{}{}
		destination.ConnectionCount++
	}
}

func protocolName(protocol uint8) string {
	switch protocol {
	case 6:
		return "tcp"
	case 17:
		return "udp"
	default:
		return "unknown"
	}
}

func parsePacket(packet []byte) (packetMetadata, bool) {
	const (
		ethernetHeaderLength = 14
		etherTypeIPv4        = 0x0800
		etherTypeIPv6        = 0x86dd
		etherTypeVLAN        = 0x8100
		etherTypeQinQ        = 0x88a8
	)
	if len(packet) < ethernetHeaderLength {
		return packetMetadata{}, false
	}
	offset := ethernetHeaderLength
	etherType := binary.BigEndian.Uint16(packet[12:14])
	for etherType == etherTypeVLAN || etherType == etherTypeQinQ {
		if len(packet) < offset+4 {
			return packetMetadata{}, false
		}
		etherType = binary.BigEndian.Uint16(packet[offset+2 : offset+4])
		offset += 4
	}
	switch etherType {
	case etherTypeIPv4:
		return parseIPv4Packet(packet[offset:])
	case etherTypeIPv6:
		return parseIPv6Packet(packet[offset:])
	default:
		return packetMetadata{}, false
	}
}

func parseIPv4Packet(packet []byte) (packetMetadata, bool) {
	if len(packet) < 20 || packet[0]>>4 != 4 {
		return packetMetadata{}, false
	}
	headerLength := int(packet[0]&0x0f) * 4
	totalLength := int(binary.BigEndian.Uint16(packet[2:4]))
	if headerLength < 20 || totalLength < headerLength || len(packet) < totalLength {
		return packetMetadata{}, false
	}
	// Only the first IP fragment contains transport ports.
	if binary.BigEndian.Uint16(packet[6:8])&0x1fff != 0 {
		return packetMetadata{}, false
	}
	protocol := packet[9]
	if protocol != 6 && protocol != 17 {
		return packetMetadata{}, false
	}
	sourceIP := netip.AddrFrom4([4]byte(packet[12:16]))
	destinationIP := netip.AddrFrom4([4]byte(packet[16:20]))
	return parseTransportPacket(packet[headerLength:totalLength], sourceIP, destinationIP, protocol, int64(totalLength))
}

func parseIPv6Packet(packet []byte) (packetMetadata, bool) {
	if len(packet) < 40 || packet[0]>>4 != 6 {
		return packetMetadata{}, false
	}
	totalLength := 40 + int(binary.BigEndian.Uint16(packet[4:6]))
	if len(packet) < totalLength {
		return packetMetadata{}, false
	}
	// Extension-header parsing is intentionally omitted for the first version.
	protocol := packet[6]
	if protocol != 6 && protocol != 17 {
		return packetMetadata{}, false
	}
	var sourceBytes, destinationBytes [16]byte
	copy(sourceBytes[:], packet[8:24])
	copy(destinationBytes[:], packet[24:40])
	return parseTransportPacket(
		packet[40:totalLength],
		netip.AddrFrom16(sourceBytes),
		netip.AddrFrom16(destinationBytes),
		protocol,
		int64(totalLength),
	)
}

func parseTransportPacket(packet []byte, sourceIP, destinationIP netip.Addr, protocol uint8, bytes int64) (packetMetadata, bool) {
	if len(packet) < 4 {
		return packetMetadata{}, false
	}
	return packetMetadata{
		sourceIP:        sourceIP,
		destinationIP:   destinationIP,
		sourcePort:      binary.BigEndian.Uint16(packet[0:2]),
		destinationPort: binary.BigEndian.Uint16(packet[2:4]),
		protocol:        protocol,
		bytes:           bytes,
	}, true
}
