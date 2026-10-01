package executorsmoke

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

// The smoke test binary doubles as the program that the executor runs for
// most actions. This keeps the suite to a single self-contained binary, which
// can be copied to a release test machine along with the executor under test,
// and avoids depending on any particular shell being installed on the host.
const toolArg = "__executor_smoke_tool__"

// MaybeRunTool runs the smoke tool and exits if the current process was
// invoked as the smoke tool by the executor. It must be called from TestMain
// before m.Run().
func MaybeRunTool() {
	if len(os.Args) < 2 || os.Args[1] != toolArg {
		return
	}
	if err := runTool(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "smoke tool: %s\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// fileSpec describes a file with deterministic, pseudo-random contents.
type fileSpec struct {
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	Seed       uint64 `json:"seed"`
	Executable bool   `json:"executable,omitempty"`
	// Mkdir makes the tool create parent directories before writing. When
	// false, the tool expects the executor to have created them.
	Mkdir bool `json:"mkdir,omitempty"`
}

// dirSpec describes a directory that must exist and be empty.
type dirSpec struct {
	Path string `json:"path"`
}

type manifest struct {
	Files     []fileSpec `json:"files"`
	EmptyDirs []dirSpec  `json:"empty_dirs"`
}

// contents returns the deterministic contents of a file spec. The same
// algorithm is used on both sides (test client and tool), so contents never
// need to be sent over the wire outside of the CAS.
func contents(seed uint64, size int64) []byte {
	r := rand.New(rand.NewPCG(seed, ^seed))
	b := make([]byte, size)
	for i := int64(0); i < size; i += 8 {
		var word [8]byte
		binary.LittleEndian.PutUint64(word[:], r.Uint64())
		copy(b[i:], word[:])
	}
	return b
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func readManifest(path string) (*manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	m := &manifest{}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("parse manifest %q: %w", path, err)
	}
	return m, nil
}

func runTool(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing subcommand")
	}
	cmd, args := args[0], args[1:]
	switch cmd {
	case "stdio":
		// stdio <stdout> <stderr>
		if len(args) != 2 {
			return fmt.Errorf("usage: stdio <stdout> <stderr>")
		}
		fmt.Fprint(os.Stdout, args[0])
		fmt.Fprint(os.Stderr, args[1])
		return nil
	case "exit":
		// exit <code>
		code, err := strconv.Atoi(args[0])
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "exiting with code %d\n", code)
		os.Exit(code)
		return nil
	case "sleep":
		// sleep <duration>
		d, err := time.ParseDuration(args[0])
		if err != nil {
			return err
		}
		time.Sleep(d)
		return nil
	case "env":
		// env <NAME>... prints NAME=value for each name, or NAME unset.
		for _, name := range args {
			if v, ok := os.LookupEnv(name); ok {
				fmt.Printf("%s=%s\n", name, v)
			} else {
				fmt.Printf("%s unset\n", name)
			}
		}
		return nil
	case "info":
		// info prints the working directory and platform of the tool.
		wd, err := os.Getwd()
		if err != nil {
			return err
		}
		fmt.Printf("cwd=%s\n", filepath.ToSlash(wd))
		fmt.Printf("platform=%s/%s\n", runtime.GOOS, runtime.GOARCH)
		return nil
	case "check-inputs":
		// check-inputs <manifest>
		m, err := readManifest(args[0])
		if err != nil {
			return err
		}
		return checkInputs(m)
	case "write-outputs":
		// write-outputs <manifest>
		m, err := readManifest(args[0])
		if err != nil {
			return err
		}
		return writeOutputs(m)
	default:
		return fmt.Errorf("unknown subcommand %q", cmd)
	}
}

func checkInputs(m *manifest) error {
	var errs []error
	for _, f := range m.Files {
		p := filepath.FromSlash(f.Path)
		b, err := os.ReadFile(p)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if want := contents(f.Seed, f.Size); !bytes.Equal(b, want) {
			errs = append(errs, fmt.Errorf("%s: got %d bytes (sha256 %s), want %d bytes (sha256 %s)", f.Path, len(b), sha256Hex(b), len(want), sha256Hex(want)))
		}
		if runtime.GOOS != "windows" {
			info, err := os.Stat(p)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if isExec := info.Mode()&0111 != 0; isExec != f.Executable {
				errs = append(errs, fmt.Errorf("%s: executable=%t, want %t (mode %s)", f.Path, isExec, f.Executable, info.Mode()))
			}
		}
	}
	for _, d := range m.EmptyDirs {
		entries, err := os.ReadDir(filepath.FromSlash(d.Path))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if len(entries) != 0 {
			errs = append(errs, fmt.Errorf("%s: expected empty directory, found %d entries", d.Path, len(entries)))
		}
	}
	if len(errs) > 0 {
		for _, err := range errs {
			fmt.Fprintln(os.Stderr, err)
		}
		return fmt.Errorf("%d input check(s) failed", len(errs))
	}
	fmt.Printf("verified %d files and %d empty dirs\n", len(m.Files), len(m.EmptyDirs))
	return nil
}

func writeOutputs(m *manifest) error {
	for _, f := range m.Files {
		p := filepath.FromSlash(f.Path)
		if f.Mkdir {
			if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
				return err
			}
		}
		mode := os.FileMode(0644)
		if f.Executable {
			mode = 0755
		}
		out, err := os.OpenFile(p, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, bytes.NewReader(contents(f.Seed, f.Size))); err != nil {
			out.Close()
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
	}
	for _, d := range m.EmptyDirs {
		if err := os.MkdirAll(filepath.FromSlash(d.Path), 0755); err != nil {
			return err
		}
	}
	fmt.Printf("wrote %d files and %d empty dirs\n", len(m.Files), len(m.EmptyDirs))
	return nil
}
