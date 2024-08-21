package filters

import (
	"regexp"
	"strings"

	"github.com/go-enry/go-enry/v2"
)

var (
	// Match `case:yes` or `case:y` and enable case-sensitive searches.
	caseMatcher = regexp.MustCompile(`case:(yes|y|no|n)`)

	// match `file:test.js`, `f:test.js`, and `path:test.js`
	fileMatcher = regexp.MustCompile(`(?:file:|f:|path:)(?P<filepath>[[:graph:]]+)`)

	// match `lang:go`, `lang:java`, etc.
	// the list of supported languages (and their aliases) is here:
	// https://github.com/github-linguist/linguist/blob/master/lib/linguist/languages.yml
	langMatcher = regexp.MustCompile(`(?:lang:)(?P<lang>[[:graph:]]+)`)

	// match `repo:buildbuddy-io` or `repo:buildbuddy-internal`
	repoMatcher = regexp.MustCompile(`(?:repo:)(?P<repo>[A-Za-z0-9\._-]+)`)
)

// TODO(tylerw): ensure that atoms inside of quotes are not parsed?

func ExtractCaseSensitivity(q string) (string, bool) {
	isCaseSensitive := false
	caseMatch := caseMatcher.FindStringSubmatch(q)
	if len(caseMatch) == 2 {
		q = caseMatcher.ReplaceAllString(q, "")
		if strings.HasPrefix(caseMatch[1], "y") {
			isCaseSensitive = true
		}
	}
	return q, isCaseSensitive
}

func ExtractFilenameFilter(q string) (string, string) {
	fileName := ""
	fileMatch := fileMatcher.FindStringSubmatch(q)
	if len(fileMatch) == 2 {
		q = fileMatcher.ReplaceAllString(q, "")
		fileName = fileMatch[1]
	}
	return q, fileName
}

func ExtractLanguageFilter(q string) (string, string) {
	language := ""
	langMatch := langMatcher.FindStringSubmatch(q)
	if len(langMatch) == 2 {
		q = langMatcher.ReplaceAllString(q, "")
		lang, ok := enry.GetLanguageByAlias(langMatch[1])
		if ok {
			language = strings.ToLower(lang)
		}
	}
	return q, language
}

func ExtractRepoFilter(q string) (string, string) {
	repo := ""
	repoMatch := repoMatcher.FindStringSubmatch(q)
	if len(repoMatch) == 2 {
		q = repoMatcher.ReplaceAllString(q, "")
		if r := repoMatch[1]; len(r) > 0 {
			repo = r
		}
	}
	return q, repo
}
