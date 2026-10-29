package update

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/signedrelease"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
)

var (
	flags         = flag.NewFlagSet("update", flag.ContinueOnError)
	Flags         = flags
	workspaceFlag = flags.Bool("workspace", false, "")
	systemFlag    = flags.Bool("system", false, "")
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
	if err := checkNotDowngrade(latestVersion, version.String(), "this bb"); err != nil {
		log.Printf("%s", err)
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
	res, err := signedrelease.Get(signedrelease.NewDownloadClient(), latestVersionURL)
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

// checkNotDowngrade returns an error if latest isn't a signed release version
// or is older than current. A current that isn't a release version (e.g.
// "unknown" for dev builds, or "" if nothing is installed) is ignored. The
// "latest" API response isn't signed, so without this an attacker could
// serve an old (but validly signed, or unsigned) release.
// currentDesc says where current came from, for the error message.
func checkNotDowngrade(latest, current, currentDesc string) error {
	lv, err := signedrelease.ParseVersion(latest)
	if err != nil {
		return err
	}
	if !signedrelease.IsSigned(latest) {
		return fmt.Errorf("refusing to update to %s: releases before %s are unsigned", latest, signedrelease.FirstSignedVersion)
	}
	if cv, err := signedrelease.ParseVersion(current); err == nil && lv.LessThan(cv) {
		return fmt.Errorf("refusing to update to %s: older than %s %s", latest, currentDesc, current)
	}
	return nil
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
		pinned := strings.TrimPrefix(versions[0], "buildbuddy-io/")
		if err := checkNotDowngrade(version, pinned, "the bb pinned in .bazelversion"); err != nil {
			return err
		}
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
	if err := checkNotDowngrade(version, installedVersion, "the bb installed at "+installPath); err != nil {
		return err
	}
	url, err := signedrelease.AssetURL(version, runtime.GOOS, runtime.GOARCH)
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
	if err := signedrelease.Download(signedrelease.NewDownloadClient(), url, tmpPath); err != nil {
		return err
	}
	installDir := filepath.Dir(installPath)
	log.Printf("Will write the CLI to %s - this may ask for your password.", installDir)
	if err := sudo("mkdir", "-p", installDir); err != nil {
		return fmt.Errorf("sudo mkdir: %s", err)
	}
	// Install to a temp file in the same dir, then rename, so that the
	// replacement is atomic. If install fails partway, try to remove the
	// staging file without prompting; this quietly does nothing if sudo
	// auth itself failed.
	stagingPath := filepath.Join(installDir, fmt.Sprintf(".bb.tmp.%d", os.Getpid()))
	if err := sudo("install", "-m", "0755", tmpPath, stagingPath); err != nil {
		exec.Command("sudo", "-n", "rm", "-f", stagingPath).Run()
		return fmt.Errorf("sudo install: %s", err)
	}
	if err := sudo("mv", "-f", stagingPath, installPath); err != nil {
		sudo("rm", "-f", stagingPath)
		return fmt.Errorf("sudo mv: %s", err)
	}
	return nil
}

func sudo(args ...string) error {
	cmd := exec.Command("sudo", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
