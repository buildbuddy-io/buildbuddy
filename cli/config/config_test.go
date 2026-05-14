package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAllFiles_NoUserConfigFilesDoesNotFail(t *testing.T) {
	home := t.TempDir()
	workspaceDir := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", "")

	configs, err := LoadAllFiles(workspaceDir)

	if err != nil {
		t.Fatalf("LoadAllFiles() returned error: %s", err)
	}
	if len(configs) != 0 {
		t.Fatalf("LoadAllFiles() returned %d configs, want 0", len(configs))
	}
	if _, err := os.Stat(filepath.Join(home, HomeRelativeUserConfigPath)); !os.IsNotExist(err) {
		t.Fatalf("expected no user config to be created, stat err=%v", err)
	}
}

func TestLoadAllFiles_NoUserConfigCandidatesDoesNotFail(t *testing.T) {
	workspaceDir := t.TempDir()
	t.Setenv("HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")

	configs, err := LoadAllFiles(workspaceDir)

	if err != nil {
		t.Fatalf("LoadAllFiles() returned error: %s", err)
	}
	if len(configs) != 0 {
		t.Fatalf("LoadAllFiles() returned %d configs, want 0", len(configs))
	}
}
