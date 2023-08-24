package build_buddy_url_test

import (
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestValidateRedirect(t *testing.T) {
	testCases := []struct {
		AppURL   string
		RedirURL string
		WantErr  bool
	}{
		{
			AppURL:   "",
			RedirURL: "/foo",
			WantErr:  false,
		},
		{
			AppURL:   "",
			RedirURL: "http://evil.com/foo",
			WantErr:  true,
		},
		{
			AppURL:   "http://localhost:8080",
			RedirURL: "/foo",
			WantErr:  false,
		},
		{
			AppURL:   "http://localhost:8080",
			RedirURL: "http://evil.com/foo",
			WantErr:  true,
		},
		{
			AppURL:   "http://localhost:8080",
			RedirURL: "http://localhost:8080/auth",
			WantErr:  false,
		},
		{
			AppURL:   "http://app.buildbuddy.io:8080",
			RedirURL: "http://sub.buildbuddy.io:8080/test",
			WantErr:  false,
		},
		{
			AppURL:   "http://app.buildbuddy.io:8080",
			RedirURL: "http://sub.buildbuddy.io.evil:8080/test",
			WantErr:  true,
		},
	}

	for _, tc := range testCases {
		u, err := url.Parse(tc.AppURL)
		require.NoError(t, err)
		flags.Set(t, "app.build_buddy_url", *u)
		err = build_buddy_url.ValidateRedirect(tc.RedirURL)
		gotErr := err != nil
		if gotErr != tc.WantErr {
			t.Fatalf("ValidateRedir(%q, %q) returned %s, wantErr: %t", tc.AppURL, tc.RedirURL, err, tc.WantErr)
		}
	}
}
