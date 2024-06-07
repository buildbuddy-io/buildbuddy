package registry

import (
	"regexp"
	"strings"
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

func parseGithubRequest(path string) (string, string, string, string) {
	urlParts := strings.Split(path, "/")
	repo := urlParts[2]
	version := urlParts[3]
	versionParts := strings.Split(version, "+")
	owner := strings.TrimPrefix(versionParts[0], "github.")
	tag := "master"
	if len(versionParts) > 1 {
		tag = versionParts[1]
	}
	return repo, owner, version, tag
}

func githubModule(path string) ([]byte, int, error) {
	repo, owner, version, tag := parseGithubRequest(path)
	moduleRegex := regexp.MustCompile(`(?s)module\(.*?\)`)
	body, status, err := request("https://raw.githubusercontent.com/" + owner + "/" + repo + "/" + tag + "/MODULE.bazel")
	if err != nil {
		return nil, status, err
	}
	if status > 300 {
		return nil, status, nil
	}

	moduleSnippet := []byte(`module(name="` + repo + `", version="` + version + `")`)

	if moduleRegex.Match(body) {
		moduleSnippet = moduleRegex.ReplaceAll(body, moduleSnippet)
	} else {
		moduleSnippet = append(moduleSnippet, []byte("\n\n")...)
		moduleSnippet = append(moduleSnippet, body...)
	}
	return moduleSnippet, 200, nil
}

func githubSource(path string) ([]byte, int, error) {
	repo, owner, _, tag := parseGithubRequest(path)
	return []byte(`{
		"integrity": "",
		"strip_prefix": "` + repo + `-` + tag + `",
		"url": "https://github.com/` + owner + `/` + repo + `/archive/` + tag + `.zip"
	}`), 200, nil
}
