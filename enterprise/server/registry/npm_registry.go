package registry

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
)

func handleNPM(path string) ([]byte, int, error) {
	urlParts := strings.Split(path, "/")

	switch urlParts[len(urlParts)-1] {
	case "MODULE.bazel":
		return npmModule(path)
	case "source.json":
		return npmSource(path)
	case "build.patch":
		return npmBuildPatch(path)
	}
	return nil, 404, nil
}

func parseNPMRequest(path string) (string, string, string, string, string) {
	urlParts := strings.Split(path, "/")

	version := urlParts[3]
	versionParts := strings.Split(version, "+")
	scope := strings.TrimPrefix(versionParts[0], "npm")
	scope = strings.TrimPrefix(scope, ".")
	pkg := urlParts[2]
	pkg = strings.TrimPrefix(pkg, scope+"_")
	tag := versionParts[1]
	moduleName := pkg
	if scope != "" {
		moduleName = scope + "_" + pkg
	}

	return moduleName, pkg, scope, version, tag
}

func buildFile(moduleName string, packageJson *NPMPackage) []byte {
	// TODO(siggisim): Add some rules here based on the package.json
	return []byte(`--- BUILD.bazel
+++ BUILD.bazel
@@ -0,0 +1,2 @@
+exports_files(glob(["**"]))
+filegroup(name="` + moduleName + `", srcs = glob(["**"]), visibility = ["//visibility:public"])`)
}

func npmBuildPatch(path string) ([]byte, int, error) {
	moduleName, pkg, scope, _, tag := parseNPMRequest(path)
	npmPackage, status, err := getPackageJSON(pkg, scope, tag)
	if err != nil || status > 300 {
		return nil, status, err
	}

	return buildFile(moduleName, npmPackage), 200, nil
}

func npmModule(path string) ([]byte, int, error) {
	moduleName, _, _, version, _ := parseNPMRequest(path)

	moduleFile := []byte(`
module(
name = "` + moduleName + `",
version = "` + version + `",
)
`)
	return moduleFile, 200, nil
}

type NPMDist struct {
	Integrity string `json:"integrity"`
	Tarball   string `json:"tarball"`
}
type NPMPackage struct {
	Name    string  `json:"name"`
	Version string  `json:"version"`
	Dist    NPMDist `json:"dist"`
}

func npmSource(path string) ([]byte, int, error) {
	moduleName, pkg, scope, _, tag := parseNPMRequest(path)

	npmPackage, status, err := getPackageJSON(pkg, scope, tag)
	if err != nil || status > 300 {
		return nil, status, err
	}

	var sha = sha256.New()
	sha.Write(buildFile(moduleName, npmPackage))
	encodedHash := base64.StdEncoding.EncodeToString(sha.Sum(nil))

	return []byte(`{
		"integrity": "` + npmPackage.Dist.Integrity + `",
		"url": "` + npmPackage.Dist.Tarball + `",
		"strip_prefix": "package",
		"patches": {
			"build.patch": "sha256-` + encodedHash + `"
		}
	}`), 200, nil
}

func getPackageJSON(pkg, scope, tag string) (*NPMPackage, int, error) {
	moduleName := pkg
	if scope != "" {
		moduleName = "@" + scope + "/" + pkg
	}

	body, status, err := request("https://registry.npmjs.org/" + moduleName + "/" + tag + "/")
	if err != nil || status > 300 {
		return nil, status, err
	}

	npmPackage := NPMPackage{}
	err = json.Unmarshal(body, &npmPackage)
	if err != nil {
		return nil, 500, err
	}
	return &npmPackage, 200, nil
}
