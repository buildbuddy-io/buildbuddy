package networking

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
)

const (
	maxObservedDestinations = 1000
	maxObservedFlows        = 10_000
	maxObservedDNSAddresses = 2000
	maxHostnamesPerAddress  = 8
)

// NetworkDestination contains connection metadata observed for one remote
// network endpoint. It intentionally does not contain any packet payloads.
type NetworkDestination struct {
	Hostname        string   `json:"hostname,omitempty"`
	Aliases         []string `json:"aliases,omitempty"`
	IP              string   `json:"ip"`
	Port            uint16   `json:"port"`
	Protocol        string   `json:"protocol"`
	BytesSent       int64    `json:"bytes_sent"`
	BytesReceived   int64    `json:"bytes_received"`
	PacketsSent     int64    `json:"packets_sent"`
	PacketsReceived int64    `json:"packets_received"`
	ConnectionCount int64    `json:"connection_count"`
}

type destinationKey struct {
	ip       netip.Addr
	port     uint16
	protocol uint8
	hostname string
}

type flowKey struct {
	ip        netip.Addr
	port      uint16
	protocol  uint8
	localPort uint16
}

type packetMetadata struct {
	sourceIP, destinationIP     netip.Addr
	sourcePort, destinationPort uint16
	protocol                    uint8
	bytes                       int64
	payload                     []byte
}

type dnsAnswer struct {
	ip       netip.Addr
	hostname string
	aliases  []string
	expires  time.Time
	observed time.Time
}

type dnsAssociation struct {
	hostname string
	aliases  []string
	expires  time.Time
	observed time.Time
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
	flows        map[flowKey]destinationKey
	dnsNames     map[netip.Addr][]dnsAssociation
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
		flows:        make(map[flowKey]destinationKey),
		dnsNames:     make(map[netip.Addr][]dnsAssociation),
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
	o.flows = make(map[flowKey]destinationKey)
	o.dnsNames = make(map[netip.Addr][]dnsAssociation)
}

// Destinations returns a stable snapshot sorted by protocol, IP, and port.
func (o *PacketObserver) Destinations() []*NetworkDestination {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make([]*NetworkDestination, 0, len(o.destinations))
	for _, d := range o.destinations {
		clone := *d
		clone.Aliases = append([]string(nil), d.Aliases...)
		sort.Strings(clone.Aliases)
		result = append(result, &clone)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Protocol != result[j].Protocol {
			return result[i].Protocol < result[j].Protocol
		}
		if result[i].IP != result[j].IP {
			return result[i].IP < result[j].IP
		}
		if result[i].Port != result[j].Port {
			return result[i].Port < result[j].Port
		}
		return result[i].Hostname < result[j].Hostname
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
	dnsAnswers := parseDNSResponse(md, received)

	remoteIP, remotePort, localPort := md.sourceIP, md.sourcePort, md.destinationPort
	if sent {
		remoteIP, remotePort, localPort = md.destinationIP, md.destinationPort, md.sourcePort
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, answer := range dnsAnswers {
		o.recordDNSAnswer(answer)
	}
	now := time.Now()
	flow := flowKey{ip: remoteIP, port: remotePort, protocol: md.protocol, localPort: localPort}
	key, flowExists := o.flows[flow]
	association := dnsAssociation{}
	if !flowExists {
		association = o.mostRecentDNSAssociation(remoteIP, now)
		key = destinationKey{
			ip:       remoteIP,
			port:     remotePort,
			protocol: md.protocol,
			hostname: association.hostname,
		}
	}
	destination, ok := o.destinations[key]
	if !ok {
		if len(o.destinations) >= maxObservedDestinations {
			return
		}
		destination = &NetworkDestination{
			Hostname: association.hostname,
			Aliases:  append([]string(nil), association.aliases...),
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
	if !flowExists && len(o.flows) < maxObservedFlows {
		o.flows[flow] = key
		destination.ConnectionCount++
	}
}

func (o *PacketObserver) mostRecentDNSAssociation(ip netip.Addr, now time.Time) dnsAssociation {
	var mostRecent dnsAssociation
	for _, association := range o.dnsNames[ip] {
		if !now.Before(association.expires) || association.observed.Before(mostRecent.observed) {
			continue
		}
		mostRecent = association
	}
	return mostRecent
}

func (o *PacketObserver) recordDNSAnswer(answer dnsAnswer) {
	if !answer.ip.IsValid() || answer.hostname == "" {
		return
	}
	associations, ok := o.dnsNames[answer.ip]
	if !ok {
		if len(o.dnsNames) >= maxObservedDNSAddresses {
			return
		}
	}
	active := associations[:0]
	for _, association := range associations {
		if answer.observed.Before(association.expires) {
			active = append(active, association)
		}
	}
	associations = active
	for i := range associations {
		association := &associations[i]
		if association.hostname != answer.hostname {
			continue
		}
		if answer.expires.After(association.expires) {
			association.expires = answer.expires
		}
		association.observed = answer.observed
		for _, alias := range answer.aliases {
			if len(association.aliases) >= maxHostnamesPerAddress {
				break
			}
			if !slices.Contains(association.aliases, alias) {
				association.aliases = append(association.aliases, alias)
			}
		}
		o.dnsNames[answer.ip] = associations
		return
	}
	if len(associations) >= maxHostnamesPerAddress {
		oldest := 0
		for i := 1; i < len(associations); i++ {
			if associations[i].observed.Before(associations[oldest].observed) {
				oldest = i
			}
		}
		associations[oldest] = dnsAssociation{
			hostname: answer.hostname,
			aliases:  append([]string(nil), answer.aliases...),
			expires:  answer.expires,
			observed: answer.observed,
		}
		o.dnsNames[answer.ip] = associations
		return
	}
	o.dnsNames[answer.ip] = append(associations, dnsAssociation{
		hostname: answer.hostname,
		aliases:  append([]string(nil), answer.aliases...),
		expires:  answer.expires,
		observed: answer.observed,
	})
}

func parseDNSResponse(md packetMetadata, received bool) []dnsAnswer {
	if !received || md.protocol != 17 || md.sourcePort != 53 || len(md.payload) == 0 {
		return nil
	}
	var message dns.Msg
	if err := message.Unpack(md.payload); err != nil || !message.Response {
		return nil
	}
	questionNames := make([]string, 0, len(message.Question))
	for _, question := range message.Question {
		if name := normalizeDNSName(question.Name); name != "" {
			questionNames = append(questionNames, name)
		}
	}
	if len(questionNames) == 0 {
		return nil
	}
	now := time.Now()
	records := append([]dns.RR(nil), message.Answer...)
	records = append(records, message.Extra...)
	aliases := make([]string, 0)
	for _, record := range records {
		cname, ok := record.(*dns.CNAME)
		if !ok {
			continue
		}
		alias := normalizeDNSName(cname.Target)
		if alias != "" && alias != questionNames[0] && len(aliases) < maxHostnamesPerAddress && !slices.Contains(aliases, alias) {
			aliases = append(aliases, alias)
		}
	}
	answers := make([]dnsAnswer, 0, len(records))
	for _, record := range records {
		var ip netip.Addr
		switch record := record.(type) {
		case *dns.A:
			ip, _ = netip.AddrFromSlice(record.A)
		case *dns.AAAA:
			ip, _ = netip.AddrFromSlice(record.AAAA)
		default:
			continue
		}
		if !ip.IsValid() {
			continue
		}
		recordAliases := append([]string(nil), aliases...)
		if name := normalizeDNSName(record.Header().Name); name != "" && name != questionNames[0] && len(recordAliases) < maxHostnamesPerAddress && !slices.Contains(recordAliases, name) {
			recordAliases = append(recordAliases, name)
		}
		ttl := time.Duration(record.Header().Ttl) * time.Second
		if ttl <= 0 {
			// A zero TTL prevents DNS caching, but the answer is still useful for
			// correlating the connection which immediately follows it.
			ttl = time.Minute
		}
		answers = append(answers, dnsAnswer{
			ip:       ip.Unmap(),
			hostname: questionNames[0],
			aliases:  recordAliases,
			expires:  now.Add(ttl),
			observed: now,
		})
	}
	return answers
}

func normalizeDNSName(name string) string {
	name = strings.ToLower(strings.TrimSuffix(name, "."))
	if len(name) > 253 {
		return ""
	}
	return name
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

func parseTransportPacket(packet []byte, sourceIP, destinationIP netip.Addr, protocol uint8, packetBytes int64) (packetMetadata, bool) {
	if len(packet) < 4 {
		return packetMetadata{}, false
	}
	md := packetMetadata{
		sourceIP:        sourceIP,
		destinationIP:   destinationIP,
		sourcePort:      binary.BigEndian.Uint16(packet[0:2]),
		destinationPort: binary.BigEndian.Uint16(packet[2:4]),
		protocol:        protocol,
		bytes:           packetBytes,
	}
	switch protocol {
	case 6:
		if len(packet) >= 20 {
			headerLength := int(packet[12]>>4) * 4
			if headerLength >= 20 && headerLength <= len(packet) {
				md.payload = packet[headerLength:]
			}
		}
	case 17:
		if len(packet) >= 8 {
			length := int(binary.BigEndian.Uint16(packet[4:6]))
			if length >= 8 && length <= len(packet) {
				md.payload = packet[8:length]
			}
		}
	}
	return md, true
}
