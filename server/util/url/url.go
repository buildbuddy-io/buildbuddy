package url

import (
	"regexp"
	"strings"
)

var (
	userPasswordRegex = regexp.MustCompile("^(https?://)(.*?:.*?@)")
)

func StripCredentials(url string) string {
	m := userPasswordRegex.FindStringSubmatch(url)
	if m != nil {
		url = strings.Replace(url, m[2], "", 1)
	}
	return url
}
