package artifacts

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/util/download"

	cmnpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// DownloadDirectoryName is the directory under BUILDBUDDY_ARTIFACTS_DIRECTORY
	// whose contents are automatically downloaded by bb remote.
	DownloadDirectoryName = "bb-download"
	// LocalOutputDirectoryName is the default local directory for downloaded
	// remote runner artifacts.
	LocalOutputDirectoryName = "bb-out"

	maxDownloadFileCount = 100
	maxDownloadSizeBytes = 100 * 1024 * 1024
)

type downloadableArtifact struct {
	file         *bespb.File
	relativePath string
}

// Download downloads bb-download artifacts associated with an invocation.
// Artifacts are written under outputRoot/invocationID.
func Download(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, bsClient bspb.ByteStreamClient, invocationID, outputRoot string) ([]string, error) {
	if err := validateInvocationID(invocationID); err != nil {
		return nil, err
	}
	downloadable, err := findDownloadableArtifacts(ctx, bbClient, invocationID)
	if err != nil {
		return nil, err
	}
	if len(downloadable) == 0 {
		return nil, nil
	}

	outputRoot, err = filepath.Abs(outputRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve artifact output directory: %w", err)
	}
	if err := os.MkdirAll(outputRoot, 0755); err != nil {
		return nil, fmt.Errorf("create artifact output directory: %w", err)
	}
	invocationOutputDir := filepath.Join(outputRoot, invocationID)
	if err := os.Mkdir(invocationOutputDir, 0755); err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("artifact output directory %q already exists; refusing to overwrite it", invocationOutputDir)
		}
		return nil, fmt.Errorf("create invocation artifact directory: %w", err)
	}
	success := false
	defer func() {
		if !success {
			os.RemoveAll(invocationOutputDir)
		}
	}()

	downloaded := make([]string, 0, len(downloadable))
	for _, artifact := range downloadable {
		outputPath := filepath.Join(invocationOutputDir, filepath.FromSlash(artifact.relativePath))
		if err := downloadFile(ctx, bsClient, artifact.file, outputPath); err != nil {
			return nil, err
		}
		downloaded = append(downloaded, outputPath)
	}
	success = true
	return downloaded, nil
}

// PrintDownloaded prints downloaded artifact paths relative to the current
// working directory when possible.
func PrintDownloaded(downloaded []string) {
	if len(downloaded) == 0 {
		return
	}
	fmt.Println("Downloaded remote artifacts:")
	cwd, _ := os.Getwd()
	for _, artifactPath := range downloaded {
		displayPath, err := filepath.Rel(cwd, artifactPath)
		if err != nil {
			displayPath = artifactPath
		}
		fmt.Printf("  %s\n", displayPath)
	}
}

func findDownloadableArtifacts(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID string) ([]downloadableArtifact, error) {
	status := cmnpb.Status_STATUS_UNSPECIFIED
	pageToken := ""
	seenPageTokens := make(map[string]struct{})
	byPath := make(map[string]downloadableArtifact)
	var totalSize int64
	for {
		if _, seen := seenPageTokens[pageToken]; seen {
			break
		}
		seenPageTokens[pageToken] = struct{}{}
		response, err := bbClient.GetTarget(ctx, &trpb.GetTargetRequest{
			InvocationId: invocationID,
			Status:       &status,
			PageToken:    pageToken,
			Filter:       DownloadDirectoryName,
		})
		if err != nil {
			return nil, fmt.Errorf("find downloadable artifacts for invocation %q: %w", invocationID, err)
		}

		nextPageToken := ""
		for _, group := range response.GetTargetGroups() {
			if group.GetNextPageToken() != "" {
				nextPageToken = group.GetNextPageToken()
			}
			for _, target := range group.GetTargets() {
				for _, file := range target.GetFiles() {
					relativePath, ok := downloadablePath(file.GetName())
					if !ok {
						continue
					}
					if file.GetUri() == "" {
						return nil, fmt.Errorf("downloadable artifact %q has no bytestream URI", file.GetName())
					}
					if _, duplicate := byPath[relativePath]; duplicate {
						return nil, fmt.Errorf("multiple remote artifacts map to %q", relativePath)
					}
					if len(byPath) >= maxDownloadFileCount {
						return nil, fmt.Errorf("too many downloadable artifacts (maximum %d)", maxDownloadFileCount)
					}
					if file.GetLength() < 0 || totalSize > maxDownloadSizeBytes-file.GetLength() {
						return nil, fmt.Errorf("downloadable artifacts are too large (maximum %d bytes)", maxDownloadSizeBytes)
					}
					totalSize += file.GetLength()
					byPath[relativePath] = downloadableArtifact{file: file, relativePath: relativePath}
				}
			}
		}
		if nextPageToken == "" {
			break
		}
		pageToken = nextPageToken
	}

	artifacts := make([]downloadableArtifact, 0, len(byPath))
	for _, artifact := range byPath {
		artifacts = append(artifacts, artifact)
	}
	sort.Slice(artifacts, func(i, j int) bool {
		return artifacts[i].relativePath < artifacts[j].relativePath
	})
	return artifacts, nil
}

func downloadablePath(name string) (string, bool) {
	name = strings.ReplaceAll(name, "\\", "/")
	prefix := DownloadDirectoryName + "/"
	if !strings.HasPrefix(name, prefix) {
		return "", false
	}
	relativePath := strings.TrimPrefix(name, prefix)
	if relativePath == "" || path.IsAbs(relativePath) || path.Clean(relativePath) != relativePath || relativePath == ".." || strings.HasPrefix(relativePath, "../") {
		return "", false
	}
	return relativePath, true
}

func downloadFile(ctx context.Context, bsClient bspb.ByteStreamClient, file *bespb.File, outputPath string) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("create output directory for %q: %w", outputPath, err)
	}
	out, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("output file %q already exists; refusing to overwrite it", outputPath)
		}
		return fmt.Errorf("create output file %q: %w", outputPath, err)
	}
	success := false
	defer func() {
		if !success {
			out.Close()
			os.Remove(outputPath)
		}
	}()
	if err := download.GetBytestreamFile(ctx, bsClient, file.GetUri(), out); err != nil {
		return fmt.Errorf("download artifact %q: %w", file.GetName(), err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close output file %q: %w", outputPath, err)
	}
	success = true
	return nil
}

func validateInvocationID(invocationID string) error {
	if invocationID == "" || invocationID == "." || invocationID == ".." ||
		strings.ContainsAny(invocationID, `/\`) || filepath.Clean(invocationID) != invocationID {
		return fmt.Errorf("invalid invocation ID %q", invocationID)
	}
	return nil
}
