package ztracing

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
)

const (
	ztracingCommitSHA = "2652640077baa8852d10f0e33a592a5085206ebe"
	ztracingURLFormat = "https://storage.googleapis.com/buildbuddy-tools/binaries/ztracing/%s/ztracing-%s.tar.gz"
	downloadTimeout   = 1 * time.Minute
)

type Installation struct {
	BinaryPath  string
	SkillDir    string
	LicensePath string
}

// Setup returns a pinned ztracing installation, downloading it when necessary.
// Only installations for the configured commit are retained.
func Setup(ctx context.Context) (*Installation, error) {
	platform := runtime.GOOS + "-" + runtime.GOARCH
	switch platform {
	case "darwin-arm64", "linux-amd64":
	default:
		return nil, fmt.Errorf("ztracing is not available for %s", platform)
	}
	downloadURL := fmt.Sprintf(ztracingURLFormat, ztracingCommitSHA, platform)
	cacheDir, err := storage.CacheDir()
	if err != nil {
		return nil, fmt.Errorf("get BuildBuddy cache directory: %w", err)
	}
	rootDir := filepath.Join(cacheDir, "tools", "ztracing")
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("create ztracing cache directory: %w", err)
	}
	if err := removeOldInstallations(rootDir, ztracingCommitSHA); err != nil {
		return nil, err
	}

	installationDir := filepath.Join(rootDir, ztracingCommitSHA, platform)
	installation := installationAt(installationDir)
	if isValidInstallation(installation) {
		if err := addToPath(installation.BinaryPath); err != nil {
			return nil, err
		}
		log.Printf("%sUsing existing ztracing installation at %s%s", terminal.Esc(90), installationDir, terminal.Esc())
		return installation, nil
	}

	if err := os.RemoveAll(installationDir); err != nil {
		return nil, fmt.Errorf("remove incomplete ztracing installation: %w", err)
	}
	if err := os.MkdirAll(installationDir, 0755); err != nil {
		return nil, fmt.Errorf("create ztracing installation directory: %w", err)
	}

	log.Printf("%sDownloading ztracing at commit %s for %s...%s", terminal.Esc(90), ztracingCommitSHA, platform, terminal.Esc())
	if err := downloadAndExtract(ctx, downloadURL, rootDir, installationDir); err != nil {
		os.RemoveAll(installationDir)
		return nil, err
	}
	if !isValidInstallation(installation) {
		os.RemoveAll(installationDir)
		return nil, fmt.Errorf("downloaded ztracing archive did not contain the expected binary, skill, and license")
	}
	if err := addToPath(installation.BinaryPath); err != nil {
		return nil, err
	}
	log.Printf("%sUsing ztracing installation at %s%s", terminal.Esc(90), installationDir, terminal.Esc())
	return installation, nil
}

func downloadAndExtract(ctx context.Context, downloadURL, tempDir, destinationDir string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return fmt.Errorf("create ztracing download request: %w", err)
	}
	client := &http.Client{Timeout: downloadTimeout}
	rsp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", downloadURL, err)
	}
	defer rsp.Body.Close()
	if rsp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: %s", downloadURL, rsp.Status)
	}

	archive, err := os.CreateTemp(tempDir, "ztracing-*.tar.gz")
	if err != nil {
		return fmt.Errorf("create temporary ztracing archive: %w", err)
	}
	archivePath := archive.Name()
	defer os.Remove(archivePath)

	if _, err := io.Copy(archive, rsp.Body); err != nil {
		archive.Close()
		return fmt.Errorf("write ztracing archive: %w", err)
	}
	if err := archive.Close(); err != nil {
		return fmt.Errorf("close ztracing archive: %w", err)
	}
	if err := extractArchive(archivePath, destinationDir); err != nil {
		return fmt.Errorf("extract ztracing archive: %w", err)
	}
	return nil
}

func extractArchive(archivePath, destinationDir string) error {
	archive, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer archive.Close()
	gzipReader, err := gzip.NewReader(archive)
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		name := filepath.Clean(filepath.FromSlash(header.Name))
		if name == "." {
			continue
		}
		if !filepath.IsLocal(name) {
			return fmt.Errorf("archive contains invalid path %q", header.Name)
		}
		path := filepath.Join(destinationDir, name)

		switch header.Typeflag {
		case tar.TypeXGlobalHeader, tar.TypeXHeader:
			continue
		case tar.TypeDir:
			if err := os.MkdirAll(path, header.FileInfo().Mode().Perm()); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				return err
			}
			file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, header.FileInfo().Mode().Perm())
			if err != nil {
				return err
			}
			if _, err := io.Copy(file, tarReader); err != nil {
				file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
			if err := os.Chmod(path, header.FileInfo().Mode().Perm()); err != nil {
				return err
			}
		default:
			return fmt.Errorf("archive contains unsupported entry %q", header.Name)
		}
	}
}

func addToPath(binaryPath string) error {
	path := filepath.Dir(binaryPath)
	if existingPath := os.Getenv("PATH"); existingPath != "" {
		path += string(os.PathListSeparator) + existingPath
	}
	if err := os.Setenv("PATH", path); err != nil {
		return fmt.Errorf("add ztracing to PATH: %w", err)
	}
	return nil
}

func installationAt(installationDir string) *Installation {
	return &Installation{
		BinaryPath:  filepath.Join(installationDir, "bin", "ztracing"),
		SkillDir:    filepath.Join(installationDir, "skills", "trace-analyzer"),
		LicensePath: filepath.Join(installationDir, "licenses", "ztracing-LICENSE"),
	}
}

// removeOldInstallations removes all ztracing installations except the one at the given commit.
func removeOldInstallations(rootDir, keep string) error {
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return fmt.Errorf("read ztracing cache directory: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() == keep {
			continue
		}
		installationPath := filepath.Join(rootDir, entry.Name())
		if err := os.RemoveAll(installationPath); err != nil {
			return fmt.Errorf("remove old ztracing installation %q: %w", installationPath, err)
		}
		log.Printf("Removed old ztracing installation %q", installationPath)
	}
	return nil
}

func isValidInstallation(installation *Installation) bool {
	binaryInfo, err := os.Stat(installation.BinaryPath)
	if err != nil {
		log.Debugf("Invalid ztracing installation: could not stat binary %q: %s", installation.BinaryPath, err)
		return false
	}
	if !binaryInfo.Mode().IsRegular() {
		log.Debugf("Invalid ztracing installation: binary %q is not a regular file", installation.BinaryPath)
		return false
	}
	if binaryInfo.Mode().Perm()&0111 == 0 {
		log.Debugf("Invalid ztracing installation: binary %q is not executable", installation.BinaryPath)
		return false
	}

	skillPath := filepath.Join(installation.SkillDir, "SKILL.md")
	skillInfo, err := os.Stat(skillPath)
	if err != nil {
		log.Debugf("Invalid ztracing installation: could not stat skill file %q: %s", skillPath, err)
		return false
	}
	if !skillInfo.Mode().IsRegular() {
		log.Debugf("Invalid ztracing installation: skill file %q is not a regular file", skillPath)
		return false
	}

	if _, err := os.Stat(installation.LicensePath); err != nil {
		log.Debugf("Invalid ztracing installation: could not stat license file %q: %s", installation.LicensePath, err)
		return false
	}
	return true
}
