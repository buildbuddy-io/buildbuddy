package url_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	urlutil "github.com/buildbuddy-io/buildbuddy/server/util/url"
)

func TestStripCredentials(t *testing.T) {
	for _, testCase := range []struct {
		url      string
		expected string
	}{
		{"https://github.com/foo/bar.git", "https://github.com/foo/bar.git"},
		{"https://USER:PASS@github.com/foo/bar.git", "https://github.com/foo/bar.git"},
		{"https://PASS:@github.com/foo/bar.git", "https://github.com/foo/bar.git"},
		{"https://:PASS@gitlab.com/foo/bar", "https://gitlab.com/foo/bar"},
		{"http://USER:PASS@github.com/foo/bar.git", "http://github.com/foo/bar.git"},
	} {
		str := urlutil.StripCredentials(testCase.url)

		assert.Equal(t, testCase.expected, str)
	}
}
