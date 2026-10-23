// rbe_image_check smoke-checks the BuildBuddy RBE image it runs in
// (gcr.io/flame-public/rbe-ubuntu{20,22,24}-04): installed tools, glibc,
// locale, the buildbuddy user and iptables setup, git over https, and that the
// CA bundle doesn't trust any distrusted root.
//
// It runs inside a candidate image, e.g. as a BuildBuddy RBE action whose
// container-image is that image. It exits 1 if any check failed, or 2 if the
// image isn't a supported Ubuntu release.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"slices"
	"strings"
	"time"
)

const (
	bundlePath = "/etc/ssl/certs/ca-certificates.crt"
	// A real bundle has well over 100 roots; fewer means it wasn't read right.
	minBundleCerts = 50
	cmdTimeout     = 30 * time.Second
	gitTimeout     = 60 * time.Second
)

// RBE actions don't inherit the image's ENV PATH, and Go doesn't default PATH
// when it's unset, so it's set explicitly. 20.04 installs Go in /usr/local/go.
const path = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/go/bin"

// distrustedRoots are SHA-256 fingerprints of the DER encoding of roots the
// image must not trust.
//
// In October 2026 eMudhra intermediate CAs were compromised
// (buildbuddy-io/buildbuddy-internal#8615); the intermediates chain to these
// roots. Patched ca-certificates (20260816ubuntu0.* on 22.04 and 24.04) drops
// them.
var distrustedRoots = map[string]string{
	"86:A1:EC:BA:08:9C:4A:8D:3B:BE:27:34:C6:12:BA:34:1D:81:3E:04:3C:F9:E8:A8:62:CD:5C:57:A3:6B:BE:6B": "emSign ECC Root CA - G3",         // eMudhra root
	"D7:A7:A0:FB:5D:7E:27:31:D7:71:E9:48:4E:BC:DE:F7:1D:5F:0C:3E:0A:29:48:78:2B:C8:3E:E0:EA:69:9E:F4": "Comodo AAA Certificate Services", // cross-signed an eMudhra intermediate
}

type family struct {
	glibc string
	// tools must be on the PATH, in addition to commonTools.
	tools []string
	// runs must succeed, in addition to commonRuns. Running a binary also
	// catches one built for the wrong arch.
	runs [][]string
	// setup is whether the image sets up the buildbuddy user and
	// iptables-legacy.
	setup bool
}

// commonTools and commonRuns are installed by every family's Dockerfile
// (dockerfiles/rbe-ubuntu*-04).
var commonTools = []string{"wget", "ed", "file", "less", "nc", "unzip", "zip", "ssh"}

var commonRuns = [][]string{
	{"gcc", "--version"},
	{"g++", "--version"},
	{"make", "--version"},
	{"python3", "--version"},
	{"git", "--version"},
	{"curl", "--version"},
	{"docker", "--version"},
}

var families = map[string]family{
	"20.04": {
		glibc: "2.31",
		tools: []string{"python2"},
		runs: [][]string{
			{"python", "--version"},
			{"go", "version"},
			{"clang", "--version"},
			{"java", "-version"},
			{"javac", "-version"},
		},
	},
	"22.04": {
		glibc: "2.35",
		tools: []string{"sudo"},
		runs:  [][]string{{"python", "--version"}},
		setup: true,
	},
	"24.04": {
		glibc: "2.39",
		tools: []string{"sudo"},
		runs:  [][]string{{"python", "--version"}, {"cmake", "--version"}},
		setup: true,
	},
}

// lookupFamily returns the Ubuntu release and family described by
// /etc/os-release contents.
func lookupFamily(osRelease []byte) (string, family, error) {
	vals := map[string]string{}
	for line := range strings.SplitSeq(string(osRelease), "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok && !strings.HasPrefix(k, "#") {
			vals[k] = strings.Trim(v, `"'`)
		}
	}
	if vals["ID"] != "ubuntu" {
		return "", family{}, fmt.Errorf("ID is %q, not ubuntu", vals["ID"])
	}
	version := vals["VERSION_ID"]
	f, ok := families[version]
	if !ok {
		return "", family{}, fmt.Errorf("unsupported Ubuntu release %q", version)
	}
	return version, f, nil
}

// fingerprint returns the SHA-256 of der in the form distrustedRoots uses.
func fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, ":")
}

// checkBundle checks a PEM CA bundle: it must be nothing but CERTIFICATE
// blocks separated by whitespace, each holding one DER SEQUENCE, at least min
// of them, and none in deny.
//
// Certificates are matched by the SHA-256 of their DER bytes. They're
// deliberately not parsed with crypto/x509, which rejects some legitimate
// roots (e.g. ones with negative serials), and not checked with x509 Verify,
// which since Go 1.27.3 blocks the eMudhra intermediates itself and so would
// pass a bundle that still trusts their roots.
func checkBundle(data []byte, deny map[string]string, min int) error {
	begin := []byte("-----BEGIN")
	n := 0
	var found []string
	rest := bytes.TrimLeft(data, " \t\r\n")
	for len(rest) > 0 {
		// pem.Decode silently skips anything that isn't a valid block,
		// including a corrupt block, so each block must start right here and
		// be the only one consumed.
		if !bytes.HasPrefix(rest, begin) {
			return fmt.Errorf("unparseable data after %d PEM blocks", n)
		}
		block, r := pem.Decode(rest)
		if block == nil || bytes.Count(rest[:len(rest)-len(r)], begin) != 1 {
			return fmt.Errorf("PEM block %d doesn't parse", n+1)
		}
		rest = bytes.TrimLeft(r, " \t\r\n")
		n++
		// OpenSSL also loads TRUSTED CERTIFICATE blocks, so any other type
		// could hide a root.
		if block.Type != "CERTIFICATE" {
			return fmt.Errorf("PEM block %d is %q, not CERTIFICATE", n, block.Type)
		}
		if len(block.Headers) > 0 {
			return fmt.Errorf("PEM block %d has headers", n)
		}
		// A root with bytes appended would hash differently, but OpenSSL may
		// still load it.
		var raw asn1.RawValue
		tail, err := asn1.Unmarshal(block.Bytes, &raw)
		if err != nil || len(tail) > 0 || raw.Class != asn1.ClassUniversal || raw.Tag != asn1.TagSequence || !raw.IsCompound {
			return fmt.Errorf("PEM block %d isn't a single DER SEQUENCE", n)
		}
		fp := fingerprint(block.Bytes)
		if name, ok := deny[fp]; ok {
			found = append(found, fmt.Sprintf("%s (%s)", name, fp))
		}
	}
	if n < min {
		return fmt.Errorf("only %d certificates (need at least %d)", n, min)
	}
	if len(found) > 0 {
		return fmt.Errorf("trusts %s", strings.Join(found, ", "))
	}
	return nil
}

// run runs a command with a timeout and returns its combined output.
func run(timeout time.Duration, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	// Don't wait forever on a child that holds the output pipe open.
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s: %w: %s", strings.Join(append([]string{name}, args...), " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func checkGlibc(want string) error {
	out, err := run(cmdTimeout, "getconf", "GNU_LIBC_VERSION")
	if err != nil {
		return err
	}
	if got := strings.TrimSpace(out); got != "glibc "+want {
		return fmt.Errorf("getconf GNU_LIBC_VERSION = %q, want glibc %s", got, want)
	}
	return nil
}

func checkLocale() error {
	out, err := run(cmdTimeout, "locale", "-a")
	if err != nil {
		return err
	}
	if !slices.Contains(strings.Fields(out), "en_US.utf8") {
		return fmt.Errorf("en_US.utf8 not in `locale -a`")
	}
	return nil
}

func checkUserGroups() error {
	u, err := user.Lookup("buildbuddy")
	if err != nil {
		return err
	}
	gids, err := u.GroupIds()
	if err != nil {
		return err
	}
	var names []string
	for _, gid := range gids {
		g, err := user.LookupGroupId(gid)
		if err != nil {
			return err
		}
		names = append(names, g.Name)
	}
	for _, want := range []string{"sudo", "docker"} {
		if !slices.Contains(names, want) {
			return fmt.Errorf("buildbuddy is in groups %v, not %s", names, want)
		}
	}
	return nil
}

func checkLegacy(name string) error {
	out, err := run(cmdTimeout, name, "--version")
	if err != nil {
		return err
	}
	if !strings.Contains(out, "(legacy)") {
		return fmt.Errorf("%s --version = %q, want (legacy)", name, strings.TrimSpace(out))
	}
	return nil
}

func checkCARoots() error {
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		return err
	}
	if err := checkBundle(data, distrustedRoots, minBundleCerts); err != nil {
		return fmt.Errorf("%s: %w", bundlePath, err)
	}
	return nil
}

func checkGit() error {
	out, err := run(gitTimeout, "git", "ls-remote", "https://github.com/bazelbuild/bazel", "HEAD")
	if err != nil {
		return err
	}
	if !strings.Contains(out, "\tHEAD") {
		return fmt.Errorf("unexpected git ls-remote output %q", out)
	}
	return nil
}

func main() {
	osRelease, err := os.ReadFile("/etc/os-release")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	version, f, err := lookupFamily(osRelease)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unsupported OS: %s\n", err)
		os.Exit(2)
	}
	os.Exit(checkImage(version, f))
}

// checkImage runs every check and returns the exit code.
func checkImage(version string, f family) int {
	os.Setenv("PATH", path)
	fmt.Printf("Checking Ubuntu %s (%s)\n", version, runtime.GOARCH)

	failed := false
	check := func(name string, fn func() error) {
		if err := fn(); err != nil {
			fmt.Printf("FAILED  %s: %s\n", name, err)
			failed = true
			return
		}
		fmt.Printf("ok      %s\n", name)
	}
	for _, t := range slices.Concat(commonTools, f.tools) {
		check(t, func() error {
			_, err := exec.LookPath(t)
			return err
		})
	}
	for _, r := range slices.Concat(commonRuns, f.runs) {
		check(strings.Join(r, " "), func() error {
			_, err := run(cmdTimeout, r[0], r[1:]...)
			return err
		})
	}
	check("glibc "+f.glibc, func() error { return checkGlibc(f.glibc) })
	check("en_US.UTF-8 locale", checkLocale)
	if f.setup {
		check("buildbuddy in sudo, docker", checkUserGroups)
		check("iptables legacy", func() error { return checkLegacy("iptables") })
		check("ip6tables legacy", func() error { return checkLegacy("ip6tables") })
	}
	check("no distrusted CA roots", checkCARoots)
	check("git ls-remote", checkGit)
	if failed {
		return 1
	}
	return 0
}
