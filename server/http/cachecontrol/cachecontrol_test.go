package cachecontrol_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/http/cachecontrol"
	"github.com/stretchr/testify/require"
)

func TestFreshnessTTL(t *testing.T) {
	now := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name          string
		header        http.Header
		wantTTL       time.Duration
		wantSpecified bool
		wantError     string
	}{
		{
			name: "no freshness headers are unspecified",
		},
		{
			name:          "max-age defines freshness",
			header:        http.Header{"Cache-Control": {"max-age=60"}},
			wantTTL:       time.Minute,
			wantSpecified: true,
		},
		{
			name: "s-maxage takes precedence for shared caches",
			header: http.Header{
				"Cache-Control": {"max-age=60, s-maxage=120"},
			},
			wantTTL:       2 * time.Minute,
			wantSpecified: true,
		},
		{
			name: "Age reduces remaining freshness",
			header: http.Header{
				"Cache-Control": {"max-age=60"},
				"Age":           {"20"},
			},
			wantTTL:       40 * time.Second,
			wantSpecified: true,
		},
		{
			name: "Date accounts for apparent age",
			header: http.Header{
				"Cache-Control": {"max-age=60"},
				"Date":          {now.Add(-20 * time.Second).Format(http.TimeFormat)},
			},
			wantTTL:       40 * time.Second,
			wantSpecified: true,
		},
		{
			name: "Expires defines freshness relative to Date",
			header: http.Header{
				"Date":    {now.Add(-20 * time.Second).Format(http.TimeFormat)},
				"Expires": {now.Add(40 * time.Second).Format(http.TimeFormat)},
			},
			wantTTL:       40 * time.Second,
			wantSpecified: true,
		},
		{
			name: "Age can exhaust explicit freshness",
			header: http.Header{
				"Cache-Control": {"max-age=60"},
				"Age":           {"90"},
			},
			wantSpecified: true,
		},
		{
			name: "no-cache requires validation",
			header: http.Header{
				"Cache-Control": {"no-cache, max-age=60"},
			},
			wantSpecified: true,
		},
		{
			name: "no-store prohibits caching",
			header: http.Header{
				"Cache-Control": {"no-store, max-age=60"},
			},
			wantSpecified: true,
		},
		{
			name: "private prohibits shared caching",
			header: http.Header{
				"Cache-Control": {"private, max-age=60"},
			},
			wantSpecified: true,
		},
		{
			name: "quoted extension can contain a comma",
			header: http.Header{
				"Cache-Control": {`extension="a,b", max-age=60`},
			},
			wantTTL:       time.Minute,
			wantSpecified: true,
		},
		{
			name: "unterminated quoted extension is invalid",
			header: http.Header{
				"Cache-Control": {`extension="a,b`},
			},
			wantError: "malformed Cache-Control quoted string",
		},
		{
			name: "duplicate max-age is invalid",
			header: http.Header{
				"Cache-Control": {"max-age=60, max-age=120"},
			},
			wantError: "max-age must have a single value",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Evaluate the response headers at a fixed time so age calculations
			// produce deterministic TTLs.
			ttl, specified, err := cachecontrol.FreshnessTTL(&http.Response{
				Header: testCase.header,
			}, now)
			if testCase.wantError != "" {
				require.EqualError(t, err, testCase.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.wantTTL, ttl)
			require.Equal(t, testCase.wantSpecified, specified)
		})
	}
}
