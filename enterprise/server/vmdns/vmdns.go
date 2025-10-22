package vmdns

import (
	"encoding/json"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/firecrackerutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/miekg/dns"
)

type DNSServer struct {
	remoteDNSClient interfaces.DNSClient
	overrides       []*networking.DNSOverride
}

func NewVMDNSServer(dnsOverrides []*networking.DNSOverride, dnsClient interfaces.DNSClient) *DNSServer {
	server := &DNSServer{overrides: dnsOverrides, remoteDNSClient: dnsClient}
	return server
}

func (s *DNSServer) Run() error {
	log.Infof("Starting vm dns server on port 53")

	mux := dns.NewServeMux()
	mux.Handle(".", s)

	serv := &dns.Server{
		Addr:    "127.0.0.1:53",
		Net:     "udp4",
		Handler: mux,
	}
	return serv.ListenAndServe()
}

func (s *DNSServer) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	for _, o := range s.overrides {
		if strings.HasSuffix(req.Question[0].Name, o.HostnameToOverride) && !strings.HasSuffix(req.Question[0].Name, o.RedirectToHostname) {
			s.overrideResponse(w, req, o.RedirectToHostname)
			return
		}
	}

	// For all other requests, forward to a remote DNS server.
	rsp, err := s.forwardToRemoteServer(req)
	if err != nil {
		s.returnFailureMsg(w, req)
		return
	}

	if err := w.WriteMsg(rsp); err != nil {
		log.Errorf("could not send dns reply: %s", err)
	}
}

// overrideResponse returns a DNS response pointing requests originally directed
// to `hostname_to_override` => `redirect_to_hostname`.
// The response also contains the A record for `redirect_to_hostname` so the
// client can follow the request to the final IP addr it should hit.
func (s *DNSServer) overrideResponse(w dns.ResponseWriter, req *dns.Msg, redirectToHostname string) {
	rspMsg := &dns.Msg{}
	rspMsg.SetReply(req)
	rspMsg.Rcode = dns.RcodeSuccess
	rspMsg.RecursionAvailable = true

	cname := &dns.CNAME{
		Hdr: dns.RR_Header{
			Name:   req.Question[0].Name,
			Rrtype: dns.TypeCNAME,
			Class:  dns.ClassINET,
			Ttl:    60,
		},
		Target: redirectToHostname,
	}

	// Get A record for `redirectToHostname` from remote DNS server.
	forwardMsg := &dns.Msg{}
	forwardMsg.SetQuestion(redirectToHostname, dns.TypeA)
	forwardMsg.RecursionDesired = true
	rsp, err := s.forwardToRemoteServer(forwardMsg)
	if err != nil {
		s.returnFailureMsg(w, req)
		return
	}

	rspMsg.Answer = append(rspMsg.Answer, cname)
	rspMsg.Answer = append(rspMsg.Answer, rsp.Answer...)

	if err := w.WriteMsg(rspMsg); err != nil {
		log.Errorf("could not send dns reply: %s", err)
	}
}

// forwardToRemoteServer forwards a DNS query to remote DNS servers. Other than configured
// queries that should be overwritten by the local DNS server, most should be
// forwarded on.
func (s *DNSServer) forwardToRemoteServer(req *dns.Msg) (*dns.Msg, error) {
	upstreams := []string{"8.8.8.8:53", "8.8.4.4:53", "1.1.1.1:53"}
	for _, upstream := range upstreams {
		rsp, _, err := s.remoteDNSClient.Exchange(req, upstream)
		if err == nil && rsp != nil && rsp.Rcode == dns.RcodeSuccess {
			return rsp, nil
		}
		log.Warningf("Forwarding req %s to %s failed or returned no answer, trying next remote DNS server", req.Question[0].Name, upstream)
	}
	return nil, status.InternalError("forwarding to all upstream dns servers failed")
}

func (s *DNSServer) returnFailureMsg(w dns.ResponseWriter, req *dns.Msg) {
	failureMsg := new(dns.Msg)
	failureMsg.SetRcode(req, dns.RcodeServerFailure)
	if err := w.WriteMsg(failureMsg); err != nil {
		log.Errorf("could not send dns reply: %s", err)
	}
}

// FetchDNSOverrides fetches DNS overrides from the metadata service.
// TODO(Maggie): Support dynamically editing DNS overrides (Be careful in cases
// where the DNS server is expected to be started or stopped)
func FetchDNSOverrides() ([]*networking.DNSOverride, error) {
	dnsOverridesJSON, err := firecrackerutil.FetchMMDSKey("dns_overrides")
	if err != nil {
		return nil, status.WrapError(err, "fetch dns_overrides from MMDS")
	}
	if len(dnsOverridesJSON) == 0 {
		return nil, nil
	}
	var dnsOverrides []*networking.DNSOverride
	if err := json.Unmarshal(dnsOverridesJSON, &dnsOverrides); err != nil {
		return nil, status.WrapError(err, "unmarshall dns_overrides")
	}
	return dnsOverrides, nil
}
