package vmdns_test

import (
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/vmdns"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	hostnameToOverride = "buildbuddy.dev."
	redirectToHostname = "proxy.dev."
	// mock IP address for `redirectToHostname`
	redirectIP             = "13.13.13.13"
	notOverwrittenHostname = "google."
	// mock IP address for `notOverwrittenHostname`
	notOverwrittenIP = "8.8.8.8"
)

// mockDNSClient implements interfaces.DNSClient.
type mockDNSClient struct{}

func (d *mockDNSClient) Exchange(m *dns.Msg, address string) (r *dns.Msg, rtt time.Duration, err error) {
	rsp := &dns.Msg{}
	rsp.SetReply(m)
	rsp.Rcode = dns.RcodeSuccess
	aRecord := &dns.A{
		Hdr: dns.RR_Header{
			Name:   m.Question[0].Name,
			Rrtype: dns.TypeA,
			Class:  dns.ClassINET,
			Ttl:    300,
		},
	}

	if m.Question[0].Name == redirectToHostname {
		aRecord.A = net.ParseIP(redirectIP)
	} else {
		aRecord.A = net.ParseIP(notOverwrittenIP)
	}
	rsp.Answer = append(rsp.Answer, aRecord)

	return rsp, 10 * time.Minute, nil
}

// mockResponseWriter implements dns.ResponseWriter.
type mockResponseWriter struct {
	dns.ResponseWriter

	response *dns.Msg
	err      error
}

func (m *mockResponseWriter) WriteMsg(msg *dns.Msg) error {
	m.response = msg
	return m.err
}

func TestOverride(t *testing.T) {
	overrides := []*networking.DNSOverride{
		{
			HostnameToOverride: hostnameToOverride,
			RedirectToHostname: redirectToHostname,
		},
	}
	server := vmdns.NewVMDNSServer(overrides, &mockDNSClient{})
	req := &dns.Msg{}
	req.SetQuestion(hostnameToOverride, dns.TypeA)
	writer := &mockResponseWriter{}
	server.ServeDNS(writer, req)

	// Expect the response to contain the overwritten entry, which should
	// contain both a CNAME pointing to `RedirectToHostname` and that hostname's
	// IP.
	require.NotNil(t, writer.response)
	assert.Equal(t, dns.RcodeSuccess, writer.response.Rcode)
	assert.Len(t, writer.response.Answer, 2)

	cnameRecord, ok := writer.response.Answer[0].(*dns.CNAME)
	require.True(t, ok)
	assert.Equal(t, hostnameToOverride, cnameRecord.Header().Name)
	assert.Equal(t, redirectToHostname, cnameRecord.Target)

	aRecord, ok := writer.response.Answer[1].(*dns.A)
	require.True(t, ok)
	assert.Equal(t, redirectToHostname, aRecord.Header().Name)
	assert.Equal(t, redirectIP, aRecord.A.String())
}

func TestNotOverride(t *testing.T) {
	overrides := []*networking.DNSOverride{
		{
			HostnameToOverride: hostnameToOverride,
			RedirectToHostname: redirectToHostname,
		},
	}
	server := vmdns.NewVMDNSServer(overrides, &mockDNSClient{})
	req := &dns.Msg{}
	req.SetQuestion("google.", dns.TypeA)
	writer := &mockResponseWriter{}
	server.ServeDNS(writer, req)

	require.NotNil(t, writer.response)
	assert.Equal(t, dns.RcodeSuccess, writer.response.Rcode)
	assert.Len(t, writer.response.Answer, 1)

	aRecord, ok := writer.response.Answer[0].(*dns.A)
	require.True(t, ok)
	assert.Equal(t, "google.", aRecord.Header().Name)
	assert.Equal(t, notOverwrittenIP, aRecord.A.String())
}

func TestRequestRedirectHostname(t *testing.T) {
	overrides := []*networking.DNSOverride{
		{
			HostnameToOverride: hostnameToOverride,
			RedirectToHostname: redirectToHostname,
		},
	}
	server := vmdns.NewVMDNSServer(overrides, &mockDNSClient{})
	req := &dns.Msg{}
	req.SetQuestion(redirectToHostname, dns.TypeA)
	writer := &mockResponseWriter{}
	server.ServeDNS(writer, req)

	require.NotNil(t, writer.response)
	assert.Equal(t, dns.RcodeSuccess, writer.response.Rcode)
	assert.Len(t, writer.response.Answer, 1)

	aRecord, ok := writer.response.Answer[0].(*dns.A)
	require.True(t, ok)
	assert.Equal(t, redirectToHostname, aRecord.Header().Name)
	assert.Equal(t, redirectIP, aRecord.A.String())
}
