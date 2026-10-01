// image_diff prints a high-level diff between two container images: their
// config, installed OS packages, and a summary of filesystem changes.
//
// Usage:
//
//	bazel run //tools/image_diff -- [flags] IMAGE_A IMAGE_B
//
// Each IMAGE can be a registry reference (gcr.io/foo/bar:tag or @sha256:...),
// docker:NAME for an image in the local Docker daemon, oci:DIR for an OCI image
// layout, or a path to an image tarball (as written by `docker save`).
package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

var (
	platformFlag = flag.String("platform", "linux/amd64", "Platform to compare when an image is a multi-platform index.")
	depth        = flag.Int("depth", 3, "Maximum directory depth used to group file changes in the summary.")
	maxGroups    = flag.Int("max_groups", 25, "Maximum number of directories to list in the file summary.")
	showNoise    = flag.Bool("show_noise", false, "Include usually-uninteresting paths (apt lists, caches, logs, ...) in the file summary.")
)

// Paths that differ between nearly any two builds of the same Dockerfile.
var noisePrefixes = []string{
	"/var/lib/apt/lists/",
	"/var/cache/",
	"/var/log/",
	"/tmp/",
	"/root/.cache/",
	"/etc/ld.so.cache",
	"/var/lib/dpkg/status-old",
	"/var/lib/dpkg/diversions-old",
	"/var/lib/dpkg/statoverride-old",
	"/etc/shadow-",
	"/etc/gshadow-",
	"/etc/passwd-",
	"/etc/group-",
	"/etc/subuid-",
	"/etc/subgid-",
	"/etc/machine-id",
	"/var/lib/dbus/machine-id",
	"/etc/cloud/build.info",
	"/run/",
	"/root/.launchpadlib/",
	"/lib/apk/db/",
	"/var/lib/dpkg/available",
	"/var/lib/dpkg/status",
	"/var/lib/dpkg/triggers/",
}

func isNoise(p string) bool {
	if strings.HasSuffix(p, ".pyc") || strings.Contains(p, "/__pycache__/") {
		return true
	}
	for _, prefix := range noisePrefixes {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

type file struct {
	typ      byte
	mode     int64
	uid, gid int
	size     int64
	link     string
	xattrs   string
	hash     [32]byte
	mtime    int64
}

type pkg struct {
	version string
	arch    string
}

type image struct {
	ref     string
	digest  string
	layers  int
	size    int64
	config  *v1.ConfigFile
	files   map[string]*file
	pkgs    map[string]pkg
	pkgKind string
	// dpkgArch is the image's native architecture, as dpkg names it.
	dpkgArch string
	// owner maps a file path to the package that installed it (dpkg only).
	owner     map[string]string
	osRelease string
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: image_diff [flags] IMAGE_A IMAGE_B\n\n")
		fmt.Fprintf(os.Stderr, "IMAGE is a registry reference, docker:NAME, oci:DIR, or an image tarball path.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(2)
	}
	platform, err := v1.ParsePlatform(*platformFlag)
	if err != nil {
		fatalf("bad --platform: %s", err)
	}

	var a, b *image
	var errA, errB error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a, errA = load(flag.Arg(0), platform) }()
	go func() { defer wg.Done(); b, errB = load(flag.Arg(1), platform) }()
	wg.Wait()
	if errA != nil {
		fatalf("%s: %s", flag.Arg(0), errA)
	}
	if errB != nil {
		fatalf("%s: %s", flag.Arg(1), errB)
	}

	printHeader(a, b)
	if a.digest == b.digest {
		fmt.Println("\nImages are identical.")
		return
	}
	diffConfig(a, b)
	diffPackages(a, b)
	diffFiles(a, b)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "image_diff: "+format+"\n", args...)
	os.Exit(1)
}

func resolve(ref string, platform *v1.Platform) (v1.Image, func(), error) {
	noop := func() {}
	switch {
	case strings.HasPrefix(ref, "docker:"):
		f, err := os.CreateTemp("", "image_diff-*.tar")
		if err != nil {
			return nil, noop, err
		}
		f.Close()
		cleanup := func() { os.Remove(f.Name()) }
		cmd := exec.Command("docker", "save", "-o", f.Name(), strings.TrimPrefix(ref, "docker:"))
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return nil, cleanup, fmt.Errorf("docker save: %w", err)
		}
		img, err := tarball.ImageFromPath(f.Name(), nil)
		return img, cleanup, err
	case strings.HasPrefix(ref, "oci:"):
		idx, err := layout.ImageIndexFromPath(strings.TrimPrefix(ref, "oci:"))
		if err != nil {
			return nil, noop, err
		}
		img, err := imageFromIndex(idx, platform)
		return img, noop, err
	}
	if st, err := os.Stat(ref); err == nil && !st.IsDir() {
		img, err := tarball.ImageFromPath(ref, nil)
		return img, noop, err
	}
	r, err := name.ParseReference(ref)
	if err != nil {
		return nil, noop, err
	}
	img, err := remote.Image(r, remote.WithAuthFromKeychain(authn.DefaultKeychain), remote.WithPlatform(*platform))
	return img, noop, err
}

// imageFromIndex picks the image for platform out of an index, descending into
// nested indexes.
func imageFromIndex(idx v1.ImageIndex, platform *v1.Platform) (v1.Image, error) {
	m, err := idx.IndexManifest()
	if err != nil {
		return nil, err
	}
	var images []v1.Descriptor
	for _, d := range m.Manifests {
		if d.MediaType.IsIndex() {
			child, err := idx.ImageIndex(d.Digest)
			if err != nil {
				return nil, err
			}
			if img, err := imageFromIndex(child, platform); err == nil {
				return img, nil
			}
			continue
		}
		if d.MediaType.IsImage() {
			images = append(images, d)
		}
	}
	for _, d := range images {
		if d.Platform != nil {
			if d.Platform.Satisfies(*platform) {
				return idx.Image(d.Digest)
			}
			continue
		}
		// No platform in the index; check the image's config instead.
		img, err := idx.Image(d.Digest)
		if err != nil {
			return nil, err
		}
		cf, err := img.ConfigFile()
		if err != nil {
			return nil, err
		}
		if p := cf.Platform(); p == nil || p.Satisfies(*platform) {
			return img, nil
		}
	}
	return nil, fmt.Errorf("no image for platform %s", platform)
}

func load(ref string, platform *v1.Platform) (*image, error) {
	img, cleanup, err := resolve(ref, platform)
	defer cleanup()
	if err != nil {
		return nil, err
	}
	out := &image{ref: ref, files: map[string]*file{}, owner: map[string]string{}}
	d, err := img.Digest()
	if err != nil {
		return nil, err
	}
	out.digest = d.String()
	if out.config, err = img.ConfigFile(); err != nil {
		return nil, err
	}
	layers, err := img.Layers()
	if err != nil {
		return nil, err
	}
	out.dpkgArch = dpkgArch(out.config.Architecture)
	out.layers = len(layers)
	for _, l := range layers {
		s, err := l.Size()
		if err != nil {
			return nil, err
		}
		out.size += s
	}

	fmt.Fprintf(os.Stderr, "Reading %s...\n", ref)
	rc := mutate.Extract(img)
	defer rc.Close()
	tr := tar.NewReader(rc)
	var dpkgStatus [][]byte
	var apkInstalled []byte
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		p := path.Clean("/" + h.Name)
		f := &file{
			typ:   h.Typeflag,
			mode:  h.Mode,
			uid:   h.Uid,
			gid:   h.Gid,
			size:  h.Size,
			link:  h.Linkname,
			mtime: h.ModTime.Unix(),
		}
		for _, k := range sortedKeys(h.PAXRecords) {
			if strings.HasPrefix(k, "SCHILY.xattr.") {
				f.xattrs += k + "=" + h.PAXRecords[k] + "\n"
			}
		}
		if h.Typeflag == tar.TypeReg {
			var buf bytes.Buffer
			keep := p == "/var/lib/dpkg/status" || strings.HasPrefix(p, "/var/lib/dpkg/status.d/") ||
				p == "/lib/apk/db/installed" || p == "/etc/os-release" || p == "/usr/lib/os-release" ||
				(strings.HasPrefix(p, "/var/lib/dpkg/info/") && strings.HasSuffix(p, ".list"))
			hasher := sha256.New()
			w := io.Writer(hasher)
			if keep {
				w = io.MultiWriter(hasher, &buf)
			}
			if _, err := io.Copy(w, tr); err != nil {
				return nil, err
			}
			copy(f.hash[:], hasher.Sum(nil))
			switch {
			case p == "/lib/apk/db/installed":
				apkInstalled = buf.Bytes()
			case p == "/etc/os-release" || p == "/usr/lib/os-release":
				if out.osRelease == "" {
					out.osRelease = prettyName(buf.Bytes())
				}
			case strings.HasPrefix(p, "/var/lib/dpkg/info/"):
				pkgName, arch, _ := strings.Cut(strings.TrimSuffix(path.Base(p), ".list"), ":")
				pkgName = dpkgKey(pkgName, arch, out.dpkgArch)
				for line := range strings.SplitSeq(buf.String(), "\n") {
					if line != "" && line != "/." {
						out.owner[line] = pkgName
					}
				}
			case keep:
				dpkgStatus = append(dpkgStatus, buf.Bytes())
			}
		}
		out.files[p] = f
	}
	switch {
	case len(dpkgStatus) > 0:
		out.pkgKind = "dpkg"
		out.pkgs = parseDpkg(dpkgStatus, out.dpkgArch)
	case apkInstalled != nil:
		out.pkgKind = "apk"
		out.pkgs = parseApk(apkInstalled, out.owner)
	}
	return out, nil
}

func prettyName(b []byte) string {
	for line := range strings.SplitSeq(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "PRETTY_NAME="); ok {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

// parseDpkg parses dpkg status files (a single /var/lib/dpkg/status, or the
// per-package files in /var/lib/dpkg/status.d/ used by distroless images).
func parseDpkg(files [][]byte, nativeArch string) map[string]pkg {
	pkgs := map[string]pkg{}
	for _, b := range files {
		for stanza := range strings.SplitSeq(string(b), "\n\n") {
			fields := map[string]string{}
			for line := range strings.SplitSeq(stanza, "\n") {
				if k, v, ok := strings.Cut(line, ": "); ok && !strings.HasPrefix(line, " ") {
					fields[k] = v
				}
			}
			if fields["Package"] == "" {
				continue
			}
			if s := fields["Status"]; s != "" && !strings.HasSuffix(s, " installed") {
				continue
			}
			arch := fields["Architecture"]
			pkgs[dpkgKey(fields["Package"], arch, nativeArch)] = pkg{version: fields["Version"], arch: arch}
		}
	}
	return pkgs
}

// parseApk parses /lib/apk/db/installed, recording which package owns each
// file in owner.
// dpkgArch returns dpkg's name for an OCI architecture.
func dpkgArch(arch string) string {
	switch arch {
	case "386":
		return "i386"
	case "arm":
		return "armhf"
	case "ppc64le":
		return "ppc64el"
	}
	return arch
}

// dpkgKey identifies an installed dpkg package by its name, qualified with
// its architecture if that isn't the image's native one (multi-arch images can
// have e.g. both libc6 and libc6:i386 installed).
func dpkgKey(name, arch, nativeArch string) string {
	if arch == "" || arch == "all" || arch == nativeArch {
		return name
	}
	return name + ":" + arch
}

func parseApk(b []byte, owner map[string]string) map[string]pkg {
	pkgs := map[string]pkg{}
	for stanza := range strings.SplitSeq(string(b), "\n\n") {
		var p pkg
		var n, dir string
		var files []string
		for line := range strings.SplitSeq(stanza, "\n") {
			switch {
			case strings.HasPrefix(line, "P:"):
				n = line[2:]
			case strings.HasPrefix(line, "V:"):
				p.version = line[2:]
			case strings.HasPrefix(line, "A:"):
				p.arch = line[2:]
			case strings.HasPrefix(line, "F:"):
				dir = line[2:]
			case strings.HasPrefix(line, "R:"):
				files = append(files, "/"+path.Join(dir, line[2:]))
			}
		}
		if n != "" {
			pkgs[n] = p
			for _, f := range files {
				owner[f] = n
			}
		}
	}
	return pkgs
}

func printHeader(a, b *image) {
	for _, x := range []struct {
		label string
		img   *image
	}{{"A", a}, {"B", b}} {
		img := x.img
		os := img.osRelease
		if os == "" {
			os = "unknown OS"
		}
		fmt.Printf("%s: %s\n   %s\n   %s/%s, %s, %d layers, %s compressed\n",
			x.label, img.ref, img.digest, img.config.OS, img.config.Architecture, os, img.layers, humanBytes(img.size))
	}
}

func section(title string) {
	fmt.Printf("\n== %s ==\n", title)
}

func diffConfig(a, b *image) {
	ca, cb := a.config.Config, b.config.Config
	var lines []string
	add := func(field, va, vb string) {
		if va != vb {
			lines = append(lines, fmt.Sprintf("  %s:\n    - %s\n    + %s", field, orNone(va), orNone(vb)))
		}
	}
	add("Platform", a.config.OS+"/"+a.config.Architecture+a.config.Variant, b.config.OS+"/"+b.config.Architecture+b.config.Variant)
	add("Entrypoint", fmt.Sprintf("%q", ca.Entrypoint), fmt.Sprintf("%q", cb.Entrypoint))
	add("Cmd", fmt.Sprintf("%q", ca.Cmd), fmt.Sprintf("%q", cb.Cmd))
	add("User", ca.User, cb.User)
	add("WorkingDir", ca.WorkingDir, cb.WorkingDir)
	add("ExposedPorts", fmt.Sprint(sortedKeys(ca.ExposedPorts)), fmt.Sprint(sortedKeys(cb.ExposedPorts)))
	add("Volumes", fmt.Sprint(sortedKeys(ca.Volumes)), fmt.Sprint(sortedKeys(cb.Volumes)))
	add("StopSignal", ca.StopSignal, cb.StopSignal)
	add("Healthcheck", healthcheck(ca.Healthcheck), healthcheck(cb.Healthcheck))

	envA, envB := map[string]string{}, map[string]string{}
	for _, e := range ca.Env {
		k, v, _ := strings.Cut(e, "=")
		envA[k] = v
	}
	for _, e := range cb.Env {
		k, v, _ := strings.Cut(e, "=")
		envB[k] = v
	}
	lines = append(lines, diffMaps("Env", envA, envB)...)
	lines = append(lines, diffMaps("Label", ca.Labels, cb.Labels)...)

	section("Config")
	if len(lines) == 0 {
		fmt.Println("  (no differences)")
		return
	}
	for _, l := range lines {
		fmt.Println(l)
	}
}

func healthcheck(h *v1.HealthConfig) string {
	if h == nil {
		return ""
	}
	return fmt.Sprintf("%q interval=%s timeout=%s start_period=%s retries=%d", h.Test, h.Interval, h.Timeout, h.StartPeriod, h.Retries)
}

func diffMaps(kind string, a, b map[string]string) []string {
	var lines []string
	keys := map[string]bool{}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	for _, k := range sortedKeys(keys) {
		va, inA := a[k]
		vb, inB := b[k]
		switch {
		case !inA:
			lines = append(lines, fmt.Sprintf("  + %s %s=%s", kind, k, vb))
		case !inB:
			lines = append(lines, fmt.Sprintf("  - %s %s=%s", kind, k, va))
		case va != vb:
			lines = append(lines, fmt.Sprintf("  ~ %s %s:\n    - %s\n    + %s", kind, k, va, vb))
		}
	}
	return lines
}

func diffPackages(a, b *image) {
	kind := a.pkgKind
	if kind == "" {
		kind = b.pkgKind
	}
	if kind == "" {
		section("Packages")
		fmt.Println("  (no dpkg or apk database found)")
		return
	}
	section(fmt.Sprintf("Packages (%s)", kind))
	if a.pkgKind != b.pkgKind {
		fmt.Printf("  A uses %s, B uses %s\n", orNone(a.pkgKind), orNone(b.pkgKind))
	}
	names := map[string]bool{}
	for n := range a.pkgs {
		names[n] = true
	}
	for n := range b.pkgs {
		names[n] = true
	}
	var added, removed, changed, same int
	for _, n := range sortedKeys(names) {
		pa, inA := a.pkgs[n]
		pb, inB := b.pkgs[n]
		switch {
		case !inA:
			added++
			fmt.Printf("  + %-40s %s\n", n, pb.version)
		case !inB:
			removed++
			fmt.Printf("  - %-40s %s\n", n, pa.version)
		case pa != pb:
			changed++
			va, vb := pa.version, pb.version
			if pa.arch != pb.arch {
				va += " (" + pa.arch + ")"
				vb += " (" + pb.arch + ")"
			}
			fmt.Printf("  ~ %-40s %s -> %s\n", n, va, vb)
		default:
			same++
		}
	}
	fmt.Printf("  %d added, %d removed, %d changed, %d unchanged\n", added, removed, changed, same)
}

type change int

const (
	added change = iota
	removed
	content  // file contents, type, or symlink target differ
	metadata // mode, owner, or xattrs (e.g. file capabilities) differ
	mtimeOnly
	numChanges
)

var changeNames = [numChanges]string{"added", "removed", "content", "mode/owner/xattrs", "mtime only"}

func classify(fa, fb *file) (change, bool) {
	switch {
	case fa == nil:
		return added, true
	case fb == nil:
		return removed, true
	case fa.typ != fb.typ || fa.link != fb.link || fa.hash != fb.hash:
		return content, true
	case fa.mode != fb.mode || fa.uid != fb.uid || fa.gid != fb.gid || fa.xattrs != fb.xattrs:
		return metadata, true
	case fa.mtime != fb.mtime && fa.typ != tar.TypeDir:
		return mtimeOnly, true
	}
	return 0, false
}

func diffFiles(a, b *image) {
	paths := map[string]bool{}
	for p := range a.files {
		paths[p] = true
	}
	for p := range b.files {
		paths[p] = true
	}

	// Package-owned changes are already explained by the package diff, so
	// they're only counted. Everything else is grouped by directory.
	changedPkg := func(p string) bool {
		owner := ownerOf(a, p)
		if owner == "" {
			owner = ownerOf(b, p)
		}
		if owner == "" {
			return false
		}
		return a.pkgs[owner] != b.pkgs[owner]
	}

	var totals, pkgTotals, noiseTotals [numChanges]int
	var sizeDelta int64
	groups := map[string]*[numChanges]int{}
	unchanged := 0
	for p := range paths {
		fa, fb := a.files[p], b.files[p]
		c, ok := classify(fa, fb)
		if !ok {
			unchanged++
			continue
		}
		totals[c]++
		if fa != nil && fa.typ == tar.TypeReg {
			sizeDelta -= fa.size
		}
		if fb != nil && fb.typ == tar.TypeReg {
			sizeDelta += fb.size
		}
		switch {
		case c == mtimeOnly:
			continue
		case !*showNoise && isNoise(p):
			noiseTotals[c]++
			continue
		case changedPkg(p):
			pkgTotals[c]++
			continue
		}
		g := group(p)
		if groups[g] == nil {
			groups[g] = &[numChanges]int{}
		}
		groups[g][c]++
	}

	section("Files")
	fmt.Printf("  %d paths in A, %d in B, %d unchanged\n", len(a.files), len(b.files), unchanged)
	var parts []string
	for c := range numChanges {
		parts = append(parts, fmt.Sprintf("%d %s", totals[c], changeNames[c]))
	}
	fmt.Printf("  %s\n", strings.Join(parts, ", "))
	fmt.Printf("  uncompressed size of regular files: %s\n", signedBytes(sizeDelta))
	if n := sum(pkgTotals); n > 0 {
		fmt.Printf("  %d of the changes are in files from changed packages\n", n)
	}
	if n := sum(noiseTotals); n > 0 {
		fmt.Printf("  %d of the changes are in caches, logs, apt lists, etc. (--show_noise to list)\n", n)
	}

	if len(groups) == 0 {
		return
	}
	type row struct {
		dir    string
		counts [numChanges]int
		total  int
	}
	var rows []row
	for d, counts := range groups {
		rows = append(rows, row{d, *counts, sum(*counts)})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].total != rows[j].total {
			return rows[i].total > rows[j].total
		}
		return rows[i].dir < rows[j].dir
	})
	fmt.Println("\n  Other changes, by directory:")
	for i, r := range rows {
		if i == *maxGroups {
			fmt.Printf("  ... and %d more directories\n", len(rows)-i)
			break
		}
		var parts []string
		for c := range mtimeOnly {
			if r.counts[c] > 0 {
				sign := map[change]string{added: "+", removed: "-", content: "~", metadata: "m"}[c]
				parts = append(parts, fmt.Sprintf("%s%d", sign, r.counts[c]))
			}
		}
		fmt.Printf("  %-50s %s\n", r.dir, strings.Join(parts, " "))
	}
}

// ownerOf returns the package that installed p, if known.
func ownerOf(img *image, p string) string {
	if o := img.owner[p]; o != "" {
		return o
	}
	// With merged /usr, dpkg records /usr/bin/foo as /bin/foo, etc.
	for _, dir := range []string{"bin", "sbin", "lib", "lib32", "lib64", "libx32"} {
		if rest, ok := strings.CutPrefix(p, "/usr/"+dir+"/"); ok {
			if o := img.owner["/"+dir+"/"+rest]; o != "" {
				return o
			}
		}
	}
	// dpkg's per-package metadata: /var/lib/dpkg/info/<pkg>[:<arch>].<ext>
	if rest, ok := strings.CutPrefix(p, "/var/lib/dpkg/info/"); ok {
		pkgName, arch, _ := strings.Cut(rest[:max(0, strings.LastIndex(rest, "."))], ":")
		if key := dpkgKey(pkgName, arch, img.dpkgArch); img.pkgs[key] != (pkg{}) {
			return key
		}
	}
	return ""
}

// group returns the directory containing p, cut to --depth components.
func group(p string) string {
	parts := strings.Split(strings.TrimPrefix(path.Dir(p), "/"), "/")
	if len(parts) > *depth {
		parts = parts[:*depth]
	}
	return path.Clean("/"+strings.Join(parts, "/")) + "/"
}

func sum(counts [numChanges]int) int {
	n := 0
	for _, c := range counts {
		n += c
	}
	return n
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func signedBytes(n int64) string {
	if n < 0 {
		return "-" + humanBytes(-n)
	}
	return "+" + humanBytes(n)
}
