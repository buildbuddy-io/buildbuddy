package install

import (
	_ "embed"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
	"github.com/mattn/go-isatty"
)

// EnsureInstalled runs the privileged setup if the tunnel is not installed.
func EnsureInstalled(cfg *tunnelconfig.Config) error {
	// Platform-specific check.
	reason, err := needed(cfg)
	if err != nil {
		return err
	}
	if reason == "" {
		return nil
	}
	if os.Geteuid() == 0 {
		// Already privileged.
		return Install(cfg)
	}
	if !isatty.IsTerminal(os.Stdin.Fd()) {
		return fmt.Errorf("the tunnel is not installed (%s); run: sudo bbaccess tunnel install", reason)
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	fmt.Printf("The tunnel needs a one-time privileged setup (%s).\n", reason)
	cmd := exec.Command("sudo", "-p", "[sudo] password for %p, to set up the bbaccess tunnel: ", exe, "tunnel", "install")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("installing the tunnel: %w", err)
	}
	return nil
}

// tunHelperBinary is the tun device helper, which we write to a root-owned
// location.
//
//go:embed tunhelper
var tunHelperBinary []byte

// installedTunHelperVersion checks what version of the tun helper is installed.
func installedTunHelperVersion() (int, error) {
	out, err := exec.Command(helperPath, "--version").Output()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(out)))
}

// tunHelperOutdated checks if the tun helper is not installed or is outdated.
// The returned message indicates which or "" if it's up to date.
func tunHelperOutdated() string {
	v, err := installedTunHelperVersion()
	if err != nil {
		return "the device helper is not installed"
	}
	if v < tunhelperutil.Version {
		return fmt.Sprintf("the device helper is out of date (version %d; this bbaccess has %d)", v, tunhelperutil.Version)
	}
	return ""
}

// writeTunHelper installs the embedded helper binary to root-owned location.
func writeTunHelper() error {
	if len(tunHelperBinary) == 0 {
		return errors.New("this bbaccess was built without the device helper")
	}
	dir := filepath.Dir(helperPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(helperPath)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name()) // a no-op once renamed
	_, err = f.Write(tunHelperBinary)
	if err == nil {
		err = f.Chmod(0o755)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("writing %s: %w", f.Name(), err)
	}
	if err := os.Rename(f.Name(), helperPath); err != nil {
		return fmt.Errorf("installing %s: %w", helperPath, err)
	}
	fmt.Printf("Installed the device helper (version %d) at %s\n", tunhelperutil.Version, helperPath)
	return nil
}

// userName describes a uid for an error message.
func userName(uid int) string {
	if u, err := user.LookupId(strconv.Itoa(uid)); err == nil {
		return u.Username
	}
	return fmt.Sprintf("uid %d", uid)
}

// splitHostPort splits a "host:port" listen address, defaulting the host to
// 127.0.0.1 when only a port is given.
func splitHostPort(listen string) (host, port string, err error) {
	h, p, err := net.SplitHostPort(listen)
	if err != nil {
		return "", "", err
	}
	if h == "" {
		h = "127.0.0.1"
	}
	return h, p, nil
}
