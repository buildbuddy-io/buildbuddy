package urlutil_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/urlutil"
	"github.com/stretchr/testify/assert"
)

func TestGetDomain(t *testing.T) {
	for _, test := range []struct {
		host, domain string
	}{
		{host: "buildbuddy.io", domain: "buildbuddy.io"},
		{host: "app.buildbuddy.io", domain: "buildbuddy.io"},
		{host: "foo.app.buildbuddy.io", domain: "buildbuddy.io"},
		{host: "app.localhost.io:8080", domain: "localhost.io"},
		{host: "localhost:8080", domain: "localhost"},
	} {
		domain := urlutil.GetDomain(test.host)
		assert.Equal(t, test.domain, domain, "GetDomain(%q)", test.host)
	}
}

func BenchmarkGetDomain(b *testing.B) {
	for _, test := range []struct {
		host, domain string
	}{
		{host: "buildbuddy.io", domain: "buildbuddy.io"},
		{host: "app.buildbuddy.io", domain: "buildbuddy.io"},
		{host: "foo.app.buildbuddy.io", domain: "buildbuddy.io"},
		{host: "app.localhost.io:8080", domain: "localhost.io"},
		{host: "localhost:8080", domain: "localhost"},
	} {
		b.Run(test.host, func(b *testing.B) {
			for b.Loop() {
				_ = urlutil.GetDomain(test.host)
			}
		})
	}
}
