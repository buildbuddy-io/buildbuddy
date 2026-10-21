package update

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
)

var (
	flags         = flag.NewFlagSet("update", flag.ContinueOnError)
	Flags         = flags
	workspaceFlag = flags.Bool("workspace", false, "")
	systemFlag    = flags.Bool("system", false, "")

	// Base URL for release assets. Overridden in tests.
	releaseDownloadBaseURL = "https://github.com/buildbuddy-io/bazel/releases/download"
)

const (
	installPath      = "/usr/local/bin/bb"
	latestVersionURL = "https://api.github.com/repos/buildbuddy-io/bazel/releases/latest"

	usage = `
usage: bb update

Updates bb to the latest version.

If --workspace is set, it updates (or adds) bb in your .bazelversion file
so that collaborators will see the new version when running with bazel.

If --system is set, it downloads the latest version to ` + installPath + `
on your system.

By default, --workspace or --system will be inferred based on whether
bb is already running under bazelisk or directly via ` + installPath + `,
respectively.
`
)

func HandleUpdate(args []string) (exitCode int, err error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		log.Printf("%s\n", usage)
		return 1, err
	}
	if !*workspaceFlag && !*systemFlag {
		if bazelisk.IsInvokedByBazelisk() {
			*workspaceFlag = true
		} else {
			*systemFlag = true
		}
	}

	latestVersion, err := fetchLatestVersion()
	if err != nil {
		log.Printf("Failed to fetch latest version: %s", err)
		return 1, nil
	}

	if *workspaceFlag {
		if err := updateWorkspaceVersion(latestVersion); err != nil {
			log.Printf("Failed to update .bazelversion: %s", err)
			return 1, nil
		}
	}
	if *systemFlag {
		if err := updateSystemInstall(latestVersion); err != nil {
			log.Printf("Failed to update system bb version: %s", err)
			return 1, nil
		}
	}
	return 0, nil
}

func fetchLatestVersion() (string, error) {
	res, err := http.Get(latestVersionURL)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	m := map[string]any{}
	if err := json.Unmarshal(b, &m); err != nil {
		return "", err
	}
	vi := m["tag_name"]
	v, ok := vi.(string)
	if !ok {
		return "", fmt.Errorf("unexpected type %T for 'tag_name' in response from %s", vi, latestVersionURL)
	}
	if v == "" {
		return "", fmt.Errorf("missing 'tag_name' key in response from %s", latestVersionURL)
	}
	return v, nil
}

// Attempts to update .bazelversion with the installed CLI version.
func updateWorkspaceVersion(version string) error {
	ws, err := workspace.Path()
	if err != nil {
		return err
	}
	bazelversionPath := filepath.Join(ws, ".bazelversion")
	versions, err := bazelisk.ParseVersionDotfile(bazelversionPath)
	if err != nil {
		return err
	}

	versionLine := "buildbuddy-io/" + version
	if len(versions) > 0 && strings.HasPrefix(versions[0], "buildbuddy-io/") {
		if versions[0] == versionLine {
			log.Printf(".bazelversion already includes %s", versionLine)
			return nil
		}
		versions[0] = versionLine
	} else {
		versions = append([]string{versionLine}, versions...)
	}
	stat, err := os.Stat(bazelversionPath)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(bazelversionPath, os.O_TRUNC|os.O_WRONLY, stat.Mode().Perm())
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write([]byte(strings.Join(versions, "\n") + "\n")); err != nil {
		return err
	}
	log.Printf("Successfully updated .bazelversion with %s", versionLine)
	return nil
}

func getInstalledVersion() (string, error) {
	_, err := os.Stat(installPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	b, err := exec.Command(installPath, "version", "--cli").Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func updateSystemInstall(version string) error {
	installedVersion, err := getInstalledVersion()
	if err != nil {
		log.Warnf("Failed to determine installed version: %s.", err)
	}
	if installedVersion == version {
		log.Printf("Version %s is already installed at %s", version, installPath)
		return nil
	}
	url, err := releaseAssetURL(version, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp("", "bb-update-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	tmpPath := filepath.Join(tmpDir, "bb")
	log.Printf("Downloading %s", url)
	if err := downloadAndVerify(newDownloadClient(), url, tmpPath); err != nil {
		return err
	}
	installDir := filepath.Dir(installPath)
	log.Printf("Will write the CLI to %s - this may ask for your password.", installDir)
	// Install to a temp file in the same dir, then rename, so that the
	// replacement is atomic.
	stagingPath := filepath.Join(installDir, ".bb.tmp")
	for _, args := range [][]string{
		{"mkdir", "-p", installDir},
		{"install", "-m", "0755", tmpPath, stagingPath},
		{"mv", "-f", stagingPath, installPath},
	} {
		if err := sudo(args...); err != nil {
			return fmt.Errorf("sudo %s: %s", strings.Join(args, " "), err)
		}
	}
	return nil
}

// Returns the download URL for the CLI release binary for the given
// version, OS, and architecture.
func releaseAssetURL(version, goos, goarch string) (string, error) {
	arch := goarch
	if goarch == "amd64" {
		arch = "x86_64"
	}
	if (goos != "linux" && goos != "darwin") || (arch != "x86_64" && arch != "arm64") {
		return "", fmt.Errorf("Could not find a CLI release for os '%s', arch '%s'", goos, goarch)
	}
	return fmt.Sprintf("%s/%s/bazel-%s-%s-%s", releaseDownloadBaseURL, version, version, goos, arch), nil
}

// Returns an HTTP client that refuses to follow redirects to non-https URLs.
func newDownloadClient() *http.Client {
	return &http.Client{CheckRedirect: checkRedirect}
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	if req.URL.Scheme != "https" {
		return fmt.Errorf("refusing redirect to non-https URL %s", req.URL)
	}
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	return nil
}

// Downloads url to dest, verifying its contents against the sha256 published
// at url + ".sha256".
func downloadAndVerify(client *http.Client, url, dest string) error {
	shaRes, err := httpGet(client, url+".sha256")
	if err != nil {
		return err
	}
	b, err := io.ReadAll(shaRes.Body)
	shaRes.Body.Close()
	if err != nil {
		return fmt.Errorf("read %s.sha256: %s", url, err)
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return fmt.Errorf("empty checksum file at %s.sha256", url)
	}
	expected := strings.ToLower(fields[0])
	if _, err := hex.DecodeString(expected); err != nil || len(expected) != 64 {
		return fmt.Errorf("malformed checksum %q at %s.sha256", fields[0], url)
	}

	res, err := httpGet(client, url)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), res.Body); err != nil {
		f.Close()
		return fmt.Errorf("download %s: %s", url, err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	if actual := hex.EncodeToString(h.Sum(nil)); actual != expected {
		return fmt.Errorf("checksum mismatch for %s: expected %s, got %s", url, expected, actual)
	}
	return nil
}

func httpGet(client *http.Client, url string) (*http.Response, error) {
	res, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		res.Body.Close()
		return nil, fmt.Errorf("GET %s: unexpected status %s", url, res.Status)
	}
	return res, nil
}

func sudo(args ...string) error {
	cmd := exec.Command("sudo", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
