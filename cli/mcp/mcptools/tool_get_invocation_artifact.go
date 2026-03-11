package mcptools

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
)

func (s *Service) getInvocationArtifact(ctx context.Context, args map[string]any) (any, error) {
	invocationRef, err := requiredString(args, "invocation_id")
	if err != nil {
		return nil, err
	}
	invocationID, err := extractInvocationID(invocationRef)
	if err != nil {
		return nil, err
	}
	bytestreamURL, err := requiredString(args, "bytestream_url")
	if err != nil {
		return nil, err
	}
	outputPath, err := requiredString(args, "output_path")
	if err != nil {
		return nil, err
	}

	downloadURL, err := s.fileDownloadURLForArtifact(invocationID, bytestreamURL)
	if err != nil {
		return nil, err
	}
	apiKey, err := requiredAPIKey()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create artifact download request: %w", err)
	}
	req.Header.Set(apiKeyHeader, apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download invocation artifact: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return nil, fmt.Errorf(
			"download invocation artifact: HTTP %d (%s): %s",
			resp.StatusCode,
			resp.Status,
			strings.TrimSpace(string(body)),
		)
	}

	absOutputPath := outputPath
	if !filepath.IsAbs(outputPath) {
		absOutputPath = filepath.Clean(outputPath)
	}
	if dir := filepath.Dir(absOutputPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create output directory %q: %w", dir, err)
		}
	}
	f, err := os.Create(absOutputPath)
	if err != nil {
		return nil, fmt.Errorf("create output file %q: %w", absOutputPath, err)
	}
	defer f.Close()
	written, err := io.Copy(f, resp.Body)
	if err != nil {
		return nil, fmt.Errorf("write artifact bytes to %q: %w", absOutputPath, err)
	}

	return map[string]any{
		"invocation_id":  invocationID,
		"bytestream_url": bytestreamURL,
		"download_url":   downloadURL,
		"output_path":    absOutputPath,
		"bytes_written":  written,
	}, nil
}

func (s *Service) fileDownloadURLForArtifact(invocationID, bytestreamURL string) (string, error) {
	if strings.TrimSpace(invocationID) == "" {
		return "", fmt.Errorf("invocation ID is required")
	}
	parsedResourceName, err := digest.ParseBytestreamURI(bytestreamURL)
	if err != nil {
		return "", err
	}
	baseURL := os.Getenv(fileDownloadBaseURLEnv)
	if strings.TrimSpace(baseURL) == "" {
		baseURL = inferFileDownloadBaseURLFromTarget(s.target)
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultFileDownloadBaseURL
	}
	parsedBaseURL, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse %s=%q: %w", fileDownloadBaseURLEnv, baseURL, err)
	}

	query := parsedBaseURL.Query()
	query.Set("invocation_id", invocationID)
	query.Set("bytestream_url", parsedResourceName.DownloadString())
	parsedBaseURL.Path = path.Join(parsedBaseURL.Path, "file/download")
	parsedBaseURL.RawQuery = query.Encode()
	return parsedBaseURL.String(), nil
}
