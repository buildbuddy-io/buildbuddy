// Package cachecontrol interprets HTTP caching headers.
package cachecontrol

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ResponseAge returns how long ago an HTTP response was generated, accounting
// for upstream cache residence reported by Date and Age.
//
// See https://www.rfc-editor.org/rfc/rfc9111.html#section-4.2.3
func ResponseAge(resp *http.Response, now time.Time) (time.Duration, error) {
	age := time.Duration(0)
	if value := resp.Header.Get("Date"); value != "" {
		date, err := http.ParseTime(value)
		if err != nil {
			return 0, fmt.Errorf("parse Date header: %w", err)
		}
		age = max(now.Sub(date), 0)
	}
	if value := resp.Header.Get("Age"); value != "" {
		headerAge, err := parseDeltaSeconds(strings.TrimSpace(value))
		if err != nil {
			return 0, fmt.Errorf("parse Age header: %w", err)
		}
		age = max(age, headerAge)
	}
	return age, nil
}

// FreshnessTTL returns how much longer a shared HTTP cache may reuse resp
// without contacting the origin server. Specified is false when the response
// contains neither an explicit lifetime nor a directive forbidding reuse,
// leaving the caller to decide whether to apply a heuristic lifetime.
//
// See https://www.rfc-editor.org/rfc/rfc9111.html#section-4.2
func FreshnessTTL(resp *http.Response, now time.Time) (ttl time.Duration, specified bool, err error) {
	parsed, err := parse(resp.Header)
	if err != nil {
		return 0, false, err
	}
	switch {
	case parsed.noStore:
		return 0, true, nil
	case parsed.noCache:
		return 0, true, nil
	case parsed.private:
		return 0, true, nil
	}
	lifetime, specified, err := freshnessLifetime(resp, parsed, now)
	if err != nil || !specified {
		return 0, specified, err
	}
	age, err := ResponseAge(resp, now)
	if err != nil {
		return 0, false, err
	}
	return max(lifetime-age, 0), true, nil
}

type directives struct {
	maxAge     time.Duration
	sMaxAge    time.Duration
	maxAgeSet  bool
	sMaxAgeSet bool
	noCache    bool
	noStore    bool
	private    bool
}

func splitDirectives(value string) ([]string, error) {
	var directives []string
	start := 0
	quoted := false
	escaped := false
	for i := range len(value) {
		switch {
		case escaped:
			escaped = false
		case quoted && value[i] == '\\':
			escaped = true
		case value[i] == '"':
			quoted = !quoted
		case value[i] == ',' && !quoted:
			directives = append(directives, value[start:i])
			start = i + 1
		}
	}
	if quoted || escaped {
		return nil, errors.New("malformed Cache-Control quoted string")
	}
	return append(directives, value[start:]), nil
}

func parseDeltaSeconds(value string) (time.Duration, error) {
	if value == "" || strings.Trim(value, "0123456789") != "" {
		return 0, fmt.Errorf("invalid delta-seconds %q", value)
	}
	seconds, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse delta-seconds %q: %w", value, err)
	}
	maxSeconds := uint64(time.Duration(1<<63-1) / time.Second)
	if seconds > maxSeconds {
		return 0, fmt.Errorf("delta-seconds %q overflows time.Duration", value)
	}
	return time.Duration(seconds) * time.Second, nil
}

func parse(header http.Header) (*directives, error) {
	parsed := &directives{}
	for _, headerValue := range header.Values("Cache-Control") {
		values, err := splitDirectives(headerValue)
		if err != nil {
			return nil, err
		}
		for _, directive := range values {
			name, value, hasValue := strings.Cut(strings.TrimSpace(directive), "=")
			name = strings.ToLower(strings.TrimSpace(name))
			switch name {
			case "":
			case "max-age":
				if !hasValue || parsed.maxAgeSet {
					return nil, errors.New("max-age must have a single value")
				}
				parsed.maxAge, err = parseDeltaSeconds(strings.TrimSpace(value))
				if err != nil {
					return nil, err
				}
				parsed.maxAgeSet = true
			case "s-maxage":
				if !hasValue || parsed.sMaxAgeSet {
					return nil, errors.New("s-maxage must have a single value")
				}
				parsed.sMaxAge, err = parseDeltaSeconds(strings.TrimSpace(value))
				if err != nil {
					return nil, err
				}
				parsed.sMaxAgeSet = true
			case "no-cache":
				parsed.noCache = true
			case "no-store":
				if hasValue {
					return nil, errors.New("no-store must not have a value")
				}
				parsed.noStore = true
			case "private":
				parsed.private = true
			}
		}
	}
	return parsed, nil
}

func freshnessLifetime(resp *http.Response, parsed *directives, now time.Time) (time.Duration, bool, error) {
	switch {
	case parsed.sMaxAgeSet:
		return parsed.sMaxAge, true, nil
	case parsed.maxAgeSet:
		return parsed.maxAge, true, nil
	case resp.Header.Get("Expires") != "":
		expires, err := http.ParseTime(resp.Header.Get("Expires"))
		if err != nil {
			return 0, false, fmt.Errorf("parse Expires header: %w", err)
		}
		date := now
		if value := resp.Header.Get("Date"); value != "" {
			date, err = http.ParseTime(value)
			if err != nil {
				return 0, false, fmt.Errorf("parse Date header: %w", err)
			}
		}
		return max(expires.Sub(date), 0), true, nil
	default:
		return 0, false, nil
	}
}
