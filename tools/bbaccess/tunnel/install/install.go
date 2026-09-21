package install

import (
	"fmt"
	"net"
	"os"
	"os/exec"

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
		// Already privileged (macOS runs the daemon under sudo).
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
