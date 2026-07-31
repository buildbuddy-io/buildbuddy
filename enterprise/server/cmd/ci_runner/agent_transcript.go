package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_env"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
)

const (
	agentTranscriptArtifactDirectoryName = "agent-transcript"
	maxAgentTranscriptArtifactBytes      = 64 << 20
	maxAgentTranscriptArtifactFileBytes  = 16 << 20
	maxAgentTranscriptArtifactFiles      = 1000
)

type agentTranscriptFileState struct {
	sizeBytes    int64
	modifiedNsec int64
}

type agentTranscriptCollector struct {
	fileStates map[string]agentTranscriptFileState
}

// preserve writes new or changed agent transcript files to the current step's
// artifact directory. The BES artifact uploader subsequently publishes these
// files as invocation artifacts, where each JSONL file can be viewed directly.
func (c *agentTranscriptCollector) preserve(artifactsDir string, redactionValues []string) (int, error) {
	type transcriptRoot struct {
		name string
		path string
	}
	roots := []transcriptRoot{
		{name: "claude", path: os.Getenv("CLAUDE_CONFIG_DIR")},
		{name: "codex", path: os.Getenv("CODEX_HOME")},
	}
	namedRedactionValues := agentTranscriptNamedRedactionValues()
	tmpDir, err := os.MkdirTemp(artifactsDir, ".agent-transcript-")
	if err != nil {
		return 0, fmt.Errorf("create temporary agent transcript artifact directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	if c.fileStates == nil {
		c.fileStates = make(map[string]agentTranscriptFileState)
	}
	pendingStates := make(map[string]agentTranscriptFileState)
	fileCount := 0
	var totalBytes int64
	for _, root := range roots {
		if root.path == "" {
			continue
		}
		if _, err := os.Stat(root.path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, fmt.Errorf("stat %s transcript directory: %w", root.name, err)
		}
		err := filepath.WalkDir(root.path, func(sourcePath string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !entry.Type().IsRegular() || !strings.EqualFold(filepath.Ext(entry.Name()), ".jsonl") {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			state := agentTranscriptFileState{
				sizeBytes:    info.Size(),
				modifiedNsec: info.ModTime().UnixNano(),
			}
			if previousState, ok := c.fileStates[sourcePath]; ok && previousState == state {
				return nil
			}
			if info.Size() < 0 || info.Size() > maxAgentTranscriptArtifactFileBytes {
				return fmt.Errorf("agent transcript file %q exceeds %d bytes", sourcePath, maxAgentTranscriptArtifactFileBytes)
			}
			fileCount++
			if fileCount > maxAgentTranscriptArtifactFiles {
				return fmt.Errorf("agent transcript artifacts exceed %d files", maxAgentTranscriptArtifactFiles)
			}
			totalBytes += info.Size()
			if totalBytes > maxAgentTranscriptArtifactBytes {
				return fmt.Errorf("agent transcript artifacts exceed %d bytes", maxAgentTranscriptArtifactBytes)
			}

			relativePath, err := filepath.Rel(root.path, sourcePath)
			if err != nil || relativePath == "." || filepath.IsAbs(relativePath) || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
				return fmt.Errorf("invalid %s transcript path %q", root.name, sourcePath)
			}
			contents, err := os.ReadFile(sourcePath)
			if err != nil {
				return err
			}
			destinationPath := filepath.Join(tmpDir, root.name, relativePath)
			if err := os.MkdirAll(filepath.Dir(destinationPath), 0o700); err != nil {
				return err
			}
			sanitized := redact.RedactTextWithNamedValues(string(contents), redactionValues, namedRedactionValues)
			if err := os.WriteFile(destinationPath, []byte(sanitized), 0o600); err != nil {
				return err
			}
			pendingStates[sourcePath] = state
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("collect %s agent transcripts: %w", root.name, err)
		}
	}

	if fileCount == 0 {
		return 0, nil
	}
	destinationRoot := filepath.Join(artifactsDir, agentTranscriptArtifactDirectoryName)
	if err := os.Rename(tmpDir, destinationRoot); err != nil {
		return 0, fmt.Errorf("publish agent transcript artifacts: %w", err)
	}
	for sourcePath, state := range pendingStates {
		c.fileStates[sourcePath] = state
	}
	return fileCount, nil
}

func agentTranscriptNamedRedactionValues() []redact.NamedRedactionValue {
	var names []string
	if err := json.Unmarshal([]byte(os.Getenv(ci_runner_env.BuildBuddySecretEnvVarNamesForRedaction)), &names); err != nil {
		return nil
	}
	namedValues := make([]redact.NamedRedactionValue, 0, len(names))
	for _, name := range names {
		if value, ok := os.LookupEnv(name); ok && name != "" && value != "" {
			namedValues = append(namedValues, redact.NamedRedactionValue{Name: name, Value: value})
		}
	}
	return namedValues
}
