package update

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
)

//go:embed install.sh
var installScript string

var (
	flags         = flag.NewFlagSet("update", flag.ContinueOnError)
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
	cmd, idx := arg.GetCommandAndIndex(args)
	if cmd != flags.Name() {
		return -1, nil
	}
	if err := arg.ParseFlagSet(flags, args[idx+1:]); err != nil {
		log.Print(err.Error())
		log.Print(usage)
		return -1, nil
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
	log.Printf("Downloading latest version.")
	if err := bash(installScript, "tags/"+version); err != nil {
		return fmt.Errorf("failed to run install script: %s", err)
	}
	return nil
}

func bash(scriptContent string, args ...string) error {
	bashArgs := append([]string{"-c", scriptContent}, args...)
	cmd := exec.Command("bash", bashArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
