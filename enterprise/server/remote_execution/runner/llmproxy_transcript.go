package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/klauspost/compress/zstd"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	agentTranscriptArtifactName    = "agent-transcript.tar.zst"
	maxAgentTranscriptArchiveBytes = 64 << 20
	maxAgentTranscriptFileBytes    = 16 << 20
	maxAgentTranscriptFiles        = 1000
)

type cappedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		return 0, status.ResourceExhaustedErrorf("agent transcript archive exceeds %d bytes", b.limit)
	}
	if len(p) > remaining {
		n, err := b.Buffer.Write(p[:remaining])
		if err != nil {
			return n, err
		}
		return n, status.ResourceExhaustedErrorf("agent transcript archive exceeds %d bytes", b.limit)
	}
	return b.Buffer.Write(p)
}

func (r *taskRunner) collectAgentTranscript(ctx context.Context) ([]byte, error) {
	if r.llmProxySession == nil || r.llmProxyConfigDir == "" {
		return nil, nil
	}

	guestConfigDir := "/tmp/" + r.llmProxyConfigDir
	collectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	var rawArchive cappedBuffer
	rawArchive.limit = maxAgentTranscriptArchiveBytes
	result := r.Container.Exec(collectCtx, &repb.Command{
		Arguments: []string{"/bin/sh", "-c", fmt.Sprintf(
			"cd %s && find claude codex -type f -name '*.jsonl' -print0 2>/dev/null | tar -cf - --null -T -",
			shlex.Quote(guestConfigDir),
		)},
	}, &interfaces.Stdio{Stdout: &rawArchive})
	if result.Error != nil {
		return nil, status.WrapError(result.Error, "collect transcript files")
	}
	if result.ExitCode != 0 {
		return nil, status.FailedPreconditionErrorf(
			"collect transcript files: exited with code %d: %s",
			result.ExitCode, strings.TrimSpace(string(result.Stderr)))
	}

	var compressed cappedBuffer
	compressed.limit = maxAgentTranscriptArchiveBytes
	zstdWriter, err := zstd.NewWriter(&compressed)
	if err != nil {
		return nil, status.WrapError(err, "create transcript compressor")
	}
	tarWriter := tar.NewWriter(zstdWriter)
	tarReader := tar.NewReader(bytes.NewReader(rawArchive.Bytes()))
	fileCount := 0
	var totalBytes int64
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, status.WrapError(err, "read transcript archive")
		}
		cleanName := path.Clean(header.Name)
		if cleanName != header.Name || path.IsAbs(cleanName) ||
			(!strings.HasPrefix(cleanName, "claude/") &&
				!strings.HasPrefix(cleanName, "codex/")) {
			return nil, status.InvalidArgumentErrorf("unexpected transcript path %q", header.Name)
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return nil, status.InvalidArgumentErrorf("unexpected transcript file type for %q", header.Name)
		}
		if path.Ext(cleanName) != ".jsonl" {
			return nil, status.InvalidArgumentErrorf("unexpected transcript file extension for %q", header.Name)
		}
		if header.Size < 0 || header.Size > maxAgentTranscriptFileBytes {
			return nil, status.ResourceExhaustedErrorf("transcript file %q exceeds %d bytes", header.Name, maxAgentTranscriptFileBytes)
		}
		fileCount++
		if fileCount > maxAgentTranscriptFiles {
			return nil, status.ResourceExhaustedErrorf("transcript archive exceeds %d files", maxAgentTranscriptFiles)
		}
		totalBytes += header.Size
		if totalBytes > maxAgentTranscriptArchiveBytes {
			return nil, status.ResourceExhaustedErrorf("transcript contents exceed %d bytes", maxAgentTranscriptArchiveBytes)
		}
		content, err := io.ReadAll(tarReader)
		if err != nil {
			return nil, status.WrapErrorf(err, "read transcript file %q", header.Name)
		}
		sanitized := []byte(redact.RedactTextWithNamedValues(
			string(content), r.llmProxySession.RedactionValues, r.llmProxySession.NamedRedactionValues))
		sanitizedHeader := &tar.Header{
			Name:     cleanName,
			Mode:     0o600,
			Size:     int64(len(sanitized)),
			ModTime:  header.ModTime,
			Typeflag: tar.TypeReg,
		}
		if err := tarWriter.WriteHeader(sanitizedHeader); err != nil {
			return nil, status.WrapError(err, "write transcript archive header")
		}
		if _, err := tarWriter.Write(sanitized); err != nil {
			return nil, status.WrapError(err, "write sanitized transcript")
		}
	}
	if err := tarWriter.Close(); err != nil {
		return nil, status.WrapError(err, "close transcript archive")
	}
	if err := zstdWriter.Close(); err != nil {
		return nil, status.WrapError(err, "close transcript compressor")
	}
	if fileCount == 0 {
		return nil, nil
	}
	return compressed.Bytes(), nil
}
