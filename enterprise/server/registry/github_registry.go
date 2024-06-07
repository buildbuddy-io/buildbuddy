package registry

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

func handleGitHub(w http.ResponseWriter, req *http.Request) {
	urlParts := strings.Split(req.URL.Path, "/")

	switch urlParts[len(urlParts)-1] {
	case "MODULE.bazel":
		githubModule(w, req)
	case "source.json":
		githubSource(w, req)
	}
}

func parseGithubRequest(req *http.Request) (string, string, string, string) {
	urlParts := strings.Split(req.URL.Path, "/")
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

func githubModule(w http.ResponseWriter, req *http.Request) {
	repo, owner, version, tag := parseGithubRequest(req)
	moduleRegex := regexp.MustCompile(`(?s)module\(.*?\)`)
	body, status, err := request("https://raw.githubusercontent.com/" + owner + "/" + repo + "/" + tag + "/MODULE.bazel")
	if err != nil {
		log.Errorf("%s", err)
		w.WriteHeader(status)
		return
	}
	if status > 300 {
		w.WriteHeader(status)
		return
	}

	moduleSnippet := []byte(`module(name="` + repo + `", version="` + version + `")`)

	if moduleRegex.Match(body) {
		moduleSnippet = moduleRegex.ReplaceAll(body, moduleSnippet)
	} else {
		moduleSnippet = append(moduleSnippet, []byte("\n\n")...)
		moduleSnippet = append(moduleSnippet, body...)
	}
	w.Write(moduleSnippet)
}

func githubSource(w http.ResponseWriter, req *http.Request) {
	repo, owner, _, tag := parseGithubRequest(req)
	w.Write([]byte(`{
		"integrity": "",
		"strip_prefix": "` + repo + `-` + tag + `",
		"url": "https://github.com/` + owner + `/` + repo + `/archive/` + tag + `.zip"
	}`))
}
