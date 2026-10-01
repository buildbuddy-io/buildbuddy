package executorsmoke

import (
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// binaryInfo describes the platform an executable was built for, as read from
// its object file headers.
type binaryInfo struct {
	OS   string
	Arch string
	// Static is true if the binary has no dynamic loader (ELF only).
	Static bool
}

// inspectBinary reads the object file headers of the given executable to
// determine which OS and architecture it targets. This works regardless of
// the host platform, so it can catch mislabeled release artifacts.
func inspectBinary(path string) (*binaryInfo, error) {
	if f, err := elf.Open(path); err == nil {
		defer f.Close()
		info := &binaryInfo{OS: "linux", Static: true}
		switch f.Machine {
		case elf.EM_X86_64:
			info.Arch = "amd64"
		case elf.EM_AARCH64:
			info.Arch = "arm64"
		case elf.EM_386:
			info.Arch = "386"
		default:
			info.Arch = f.Machine.String()
		}
		for _, p := range f.Progs {
			if p.Type == elf.PT_INTERP {
				info.Static = false
			}
		}
		return info, nil
	}
	if f, err := macho.Open(path); err == nil {
		defer f.Close()
		return &binaryInfo{OS: "darwin", Arch: machoArch(f.Cpu)}, nil
	}
	if ff, err := macho.OpenFat(path); err == nil {
		defer ff.Close()
		// Universal binaries are not expected, but report the first arch.
		return &binaryInfo{OS: "darwin", Arch: machoArch(ff.Arches[0].Cpu)}, nil
	}
	if f, err := pe.Open(path); err == nil {
		defer f.Close()
		info := &binaryInfo{OS: "windows"}
		switch f.Machine {
		case pe.IMAGE_FILE_MACHINE_AMD64:
			info.Arch = "amd64"
		case pe.IMAGE_FILE_MACHINE_ARM64:
			info.Arch = "arm64"
		case pe.IMAGE_FILE_MACHINE_I386:
			info.Arch = "386"
		default:
			info.Arch = fmt.Sprintf("pe-machine-%#x", f.Machine)
		}
		return info, nil
	}
	return nil, fmt.Errorf("%s is not a recognized ELF, Mach-O, or PE executable", path)
}

func machoArch(cpu macho.Cpu) string {
	switch cpu {
	case macho.CpuAmd64:
		return "amd64"
	case macho.CpuArm64:
		return "arm64"
	default:
		return cpu.String()
	}
}

// resolveExecutorBinary returns a local path to the executor binary. If the
// given location is an http(s) URL (e.g. a GitHub release asset), it is
// downloaded into dir first.
func resolveExecutorBinary(location, dir string) (string, error) {
	if !strings.HasPrefix(location, "http://") && !strings.HasPrefix(location, "https://") {
		abs, err := filepath.Abs(location)
		if err != nil {
			return "", err
		}
		if _, err := os.Stat(abs); err != nil {
			return "", fmt.Errorf("executor binary: %w", err)
		}
		return abs, nil
	}
	name := "executor"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(dir, name)
	rsp, err := http.Get(location)
	if err != nil {
		return "", fmt.Errorf("download executor: %w", err)
	}
	defer rsp.Body.Close()
	if rsp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download executor from %s: HTTP %s", location, rsp.Status)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, rsp.Body); err != nil {
		f.Close()
		return "", fmt.Errorf("download executor: %w", err)
	}
	return path, f.Close()
}
