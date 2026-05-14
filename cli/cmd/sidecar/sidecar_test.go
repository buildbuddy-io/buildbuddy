package sidecar

import "testing"

func TestNormalizeGrpcTarget(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{"grpc passthrough", "grpc://host", "grpc://host"},
		{"grpcs passthrough", "grpcs://host", "grpcs://host"},
		{"http translated to grpc", "http://host", "grpc://host"},
		{"https translated to grpcs", "https://host", "grpcs://host"},
		{"https with userinfo", "https://u:p@host", "grpcs://u:p@host"},
		{"http with userinfo and port", "http://u:p@host:1234", "grpc://u:p@host:1234"},
		{"bare hostname defaults to grpcs", "host:443", "grpcs://host:443"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeGrpcTarget(tc.in); got != tc.want {
				t.Errorf("normalizeGrpcTarget(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
