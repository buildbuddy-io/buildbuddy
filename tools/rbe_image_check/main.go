// rbe_image_check smoke-checks the BuildBuddy RBE image it runs in
// (gcr.io/flame-public/rbe-ubuntu{20,22,24}-04). It checks that the tools the
// image's Dockerfile installs are present (and that the ones downloaded outside
// apt run), the glibc version, the en_US.UTF-8 locale, the buildbuddy user and
// iptables setup, that `git ls-remote` against GitHub works, and that the CA
// bundle doesn't trust any distrusted root.
//
// It's meant to run inside a candidate image, e.g. as a BuildBuddy RBE action
// whose container-image is that image. It prints one line per check and exits
// 1 if any check failed, or 2 if the image isn't a supported Ubuntu release.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
// when it's unset, so the PATH is set explicitly for lookups and children.
var basePath = []string{"/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin"}

// distrustedRoots are SHA-256 fingerprints of the DER encoding of roots the
// image must not trust. To distrust another root, add a line.
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
	glibc     string
	tools     []string
	extraPath []string
	// runs are run once each, to catch binaries built for the wrong arch.
	runs [][]string
	// setup is whether the image sets up the buildbuddy user and
	// iptables-legacy.
	setup bool
}

// commonTools are installed by every family's Dockerfile
// (dockerfiles/rbe-ubuntu*-04).
var commonTools = []string{"gcc", "g++", "make", "python3", "git", "curl", "wget", "ed", "file", "less", "nc", "unzip", "zip", "ssh", "docker"}

var families = map[string]family{
	"20.04": {
		glibc:     "2.31",
		tools:     append(slices.Clone(commonTools), "python2", "python", "java", "javac", "clang", "go"),
		extraPath: []string{"/usr/local/go/bin"},
		runs: [][]string{
			{"go", "version"},
			{"clang", "--version"},
			{"java", "-version"},
			{"javac", "-version"},
		},
	},
	"22.04": {
		glibc: "2.35",
		tools: append(slices.Clone(commonTools), "python", "sudo"),
		setup: true,
	},
	"24.04": {
		glibc: "2.39",
		tools: append(slices.Clone(commonTools), "python", "sudo", "cmake"),
		setup: true,
	},
}

// parseOSRelease returns the VERSION_ID from /etc/os-release contents, and
// fails if ID isn't ubuntu.
func parseOSRelease(data []byte) (string, error) {
	vals := map[string]string{}
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		vals[k] = strings.Trim(v, `"'`)
	}
	if vals["ID"] != "ubuntu" {
		return "", fmt.Errorf("ID is %q, not ubuntu", vals["ID"])
	}
	if vals["VERSION_ID"] == "" {
		return "", fmt.Errorf("no VERSION_ID")
	}
	return vals["VERSION_ID"], nil
}

// lookupFamily returns the release and family described by /etc/os-release
// contents.
func lookupFamily(osRelease []byte) (string, family, error) {
	version, err := parseOSRelease(osRelease)
	if err != nil {
		return "", family{}, err
	}
	f, ok := families[version]
	if !ok {
		return "", family{}, fmt.Errorf("unsupported Ubuntu release %q", version)
	}
	return version, f, nil
}

// parseGlibcVersion parses `getconf GNU_LIBC_VERSION` output ("glibc 2.35").
func parseGlibcVersion(out string) (string, error) {
	fields := strings.Fields(out)
	if len(fields) != 2 || fields[0] != "glibc" {
		return "", fmt.Errorf("unexpected getconf output %q", out)
	}
	return fields[1], nil
}

// hasLocale reports whether `locale -a` output lists want, ignoring case.
func hasLocale(out, want string) bool {
	for line := range strings.SplitSeq(out, "\n") {
		if strings.EqualFold(strings.TrimSpace(line), want) {
			return true
		}
	}
	return false
}

// parseFingerprint parses a colon-separated hex SHA-256 fingerprint.
func parseFingerprint(s string) ([32]byte, error) {
	var fp [32]byte
	b, err := hex.DecodeString(strings.ReplaceAll(s, ":", ""))
	if err != nil {
		return fp, fmt.Errorf("fingerprint %q: %w", s, err)
	}
	if len(b) != len(fp) {
		return fp, fmt.Errorf("fingerprint %q has %d bytes, want %d", s, len(b), len(fp))
	}
	copy(fp[:], b)
	return fp, nil
}

func parseDenylist(m map[string]string) (map[[32]byte]string, error) {
	deny := make(map[[32]byte]string, len(m))
	for s, name := range m {
		fp, err := parseFingerprint(s)
		if err != nil {
			return nil, err
		}
		deny[fp] = name
	}
	return deny, nil
}

// checkBundle checks a PEM CA bundle: every block must be a CERTIFICATE, the
// whole bundle must parse, it must hold at least min certificates, and none of
// them may be in deny.
//
// Certificates are matched by the SHA-256 of their DER bytes. They're
// deliberately not parsed with crypto/x509, which rejects some legitimate
// roots (e.g. ones with negative serials), and not checked with x509 Verify,
// which since Go 1.27.3 blocks the eMudhra intermediates itself and so would
// pass a bundle that still trusts their roots.
func checkBundle(data []byte, deny map[[32]byte]string, min int) error {
	n := 0
	var found []string
	rest := data
	for {
		block, r := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = r
		n++
		// OpenSSL also loads TRUSTED CERTIFICATE blocks, so any other type
		// could hide a root.
		if block.Type != "CERTIFICATE" {
			return fmt.Errorf("PEM block %d is %q, not CERTIFICATE", n, block.Type)
		}
		fp := sha256.Sum256(block.Bytes)
		if name, ok := deny[fp]; ok {
			found = append(found, fmt.Sprintf("%s (%X)", name, fp[:]))
		}
	}
	if len(bytes.TrimSpace(rest)) > 0 {
		return fmt.Errorf("unparseable data after PEM block %d", n)
	}
	// pem.Decode skips a malformed block and goes on to the next one.
	if begins := bytes.Count(data, []byte("-----BEGIN")); begins != n {
		return fmt.Errorf("%d of %d PEM blocks don't parse", begins-n, begins)
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
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
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
	got, err := parseGlibcVersion(out)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("glibc %s, want %s", got, want)
	}
	return nil
}

func checkLocale() error {
	out, err := run(cmdTimeout, "locale", "-a")
	if err != nil {
		return err
	}
	if !hasLocale(out, "en_US.utf8") {
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

func checkAlternative(link, want string) error {
	got, err := os.Readlink(link)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%s -> %s, want %s", link, got, want)
	}
	fi, err := os.Stat(want)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%s is not an executable file", want)
	}
	return nil
}

func checkCARoots() error {
	deny, err := parseDenylist(distrustedRoots)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		return err
	}
	if err := checkBundle(data, deny, minBundleCerts); err != nil {
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
	if err == nil {
		var version string
		var f family
		version, f, err = lookupFamily(osRelease)
		if err == nil {
			os.Exit(checkImage(version, f))
		}
	}
	fmt.Fprintf(os.Stderr, "unsupported OS: %s\n", err)
	os.Exit(2)
}

// checkImage runs every check and returns the exit code.
func checkImage(version string, f family) int {
	os.Setenv("PATH", strings.Join(append(slices.Clone(basePath), f.extraPath...), ":"))
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
	for _, t := range f.tools {
		check(t, func() error {
			_, err := exec.LookPath(t)
			return err
		})
	}
	for _, r := range f.runs {
		check(strings.Join(r, " "), func() error {
			_, err := run(cmdTimeout, r[0], r[1:]...)
			return err
		})
	}
	check("glibc "+f.glibc, func() error { return checkGlibc(f.glibc) })
	check("en_US.UTF-8 locale", checkLocale)
	if f.setup {
		check("buildbuddy in sudo, docker", checkUserGroups)
		check("iptables-legacy", func() error {
			return checkAlternative("/etc/alternatives/iptables", "/usr/sbin/iptables-legacy")
		})
		check("ip6tables-legacy", func() error {
			return checkAlternative("/etc/alternatives/ip6tables", "/usr/sbin/ip6tables-legacy")
		})
	}
	check("no distrusted CA roots", checkCARoots)
	check("git ls-remote", checkGit)
	if failed {
		return 1
	}
	return 0
}
