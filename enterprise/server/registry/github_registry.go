package registry

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

var (
	githubPathPartRegex = regexp.MustCompile(`^[A-Za-z0-9._!%'"()<>-]*$`)
	moduleRegex         = regexp.MustCompile(`(?s)module\(.*?\)`)

	// Character class for backslash and quote
	starlarkStringEscapedCharacters = regexp.MustCompile("[\\\\\"]")
)

func handleGitHub(path string) ([]byte, int, error) {
	urlParts := strings.Split(path, "/")

	switch urlParts[len(urlParts)-1] {
	case "MODULE.bazel":
		return githubModule(path)
	case "source.json":
		return githubSource(path)
	}
	return nil, 404, nil
}

func parseGithubRequest(path string) (string, string, string, string, error) {
	// "/modules/repo/version/.*" -> "", "modules/repo/version/.*"
	_, remainingPath, _ := strings.Cut(path, "/")
	// "modules/repo/version/.*" -> "modules", "repo/version/.*"
	_, remainingPath, _ = strings.Cut(remainingPath, "/")
	// "repo/version/.*" -> "repo", "version/.*"
	repo, version, _ := strings.Cut(remainingPath, "/")
	// "version/.*" -> "version", ".*"
	version, _, _ = strings.Cut(version, "/")
	if !githubPathPartRegex.MatchString(repo) || !githubPathPartRegex.MatchString(version) {
		return "", "", "", "", fmt.Errorf("Invalid path: %s", path)
	}

	// Format: {tag}-github.{owner} (e.g., "v1.0.0-github.buildbuddy-io")
	tag, owner, _ := strings.Cut(version, "-github.")
	return repo, owner, version, tag, nil
}

func escapeStringForStarlark(s string) string {
	return string(starlarkStringEscapedCharacters.ReplaceAll([]byte(s), []byte("\\$0")))
}

func moduleSnippet(repo, version string, body []byte) []byte {
	moduleSnippet := []byte(`module(name="` + escapeStringForStarlark(repo) + `", version="` + escapeStringForStarlark(version) + `")`)

	if moduleRegex.Match(body) {
		moduleSnippet = moduleRegex.ReplaceAll(body, moduleSnippet)
	} else {
		moduleSnippet = append(moduleSnippet, []byte("\n\n")...)
		moduleSnippet = append(moduleSnippet, body...)
	}
	return moduleSnippet
}

func githubModule(path string) ([]byte, int, error) {
	repo, owner, version, tag, err := parseGithubRequest(path)
	if err != nil {
		return nil, 400, err
	}
	body, status, err := request("https://raw.githubusercontent.com/" + owner + "/" + repo + "/" + tag + "/MODULE.bazel")
	if err != nil {
		return nil, status, err
	}
	if status > 300 {
		return nil, status, nil
	}

	return moduleSnippet(repo, version, body), 200, nil
}

func githubSource(path string) ([]byte, int, error) {
	repo, owner, _, tag, err := parseGithubRequest(path)
	if err != nil {
		return nil, 400, err
	}
	encoded, err := json.Marshal(map[string]string{
		"integrity":    "",
		"strip_prefix": repo + "-" + tag,
		"url":          "https://github.com/" + owner + "/" + repo + "/archive/" + tag + ".zip",
	})
	if err != nil {
		return nil, 400, err
	}
	return encoded, 200, nil
}
