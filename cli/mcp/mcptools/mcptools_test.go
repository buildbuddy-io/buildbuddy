package mcptools

import (
	"reflect"
	"testing"
)

func TestParseGitHubOwnerRepo(t *testing.T) {
	tests := []struct {
		name      string
		remoteURL string
		wantOwner string
		wantRepo  string
		wantOK    bool
	}{
		{
			name:      "scp-like ssh",
			remoteURL: "git@github.com:buildbuddy-io/buildbuddy.git",
			wantOwner: "buildbuddy-io",
			wantRepo:  "buildbuddy",
			wantOK:    true,
		},
		{
			name:      "ssh url",
			remoteURL: "ssh://git@github.com/buildbuddy-io/buildbuddy.git",
			wantOwner: "buildbuddy-io",
			wantRepo:  "buildbuddy",
			wantOK:    true,
		},
		{
			name:      "https url",
			remoteURL: "https://github.com/buildbuddy-io/buildbuddy",
			wantOwner: "buildbuddy-io",
			wantRepo:  "buildbuddy",
			wantOK:    true,
		},
		{
			name:      "non github host",
			remoteURL: "https://example.com/buildbuddy-io/buildbuddy",
			wantOwner: "",
			wantRepo:  "",
			wantOK:    false,
		},
		{
			name:      "empty",
			remoteURL: "",
			wantOwner: "",
			wantRepo:  "",
			wantOK:    false,
		},
	}
	for _, test := range tests {
		owner, repo, ok := parseGitHubOwnerRepo(test.remoteURL)
		if ok != test.wantOK || owner != test.wantOwner || repo != test.wantRepo {
			t.Fatalf(
				"%s: parseGitHubOwnerRepo(%q) = (%q, %q, %t), want (%q, %q, %t)",
				test.name,
				test.remoteURL,
				owner,
				repo,
				ok,
				test.wantOwner,
				test.wantRepo,
				test.wantOK,
			)
		}
	}
}

func TestRequiredChecksFromBranchProtection(t *testing.T) {
	protection := &githubBranchProtectionResponse{
		RequiredStatusChecks: &githubRequiredStatusChecks{
			Contexts: []string{"Test", "Lint"},
			Checks: []*githubRequiredCheckRecord{
				{Context: "Test"},
				{Context: "Darwin"},
				nil,
			},
		},
	}
	got := requiredChecksFromBranchProtection(protection)
	want := []string{"Darwin", "Lint", "Test"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("requiredChecksFromBranchProtection() = %v, want %v", got, want)
	}
}

func TestExtractBuildBuddyWorkflowActionNames(t *testing.T) {
	input := `
actions:
  - name: Test
  - name: "Check style"
  - name: Test
plugins:
  - path: cli/plugins/foo
`
	got := extractBuildBuddyWorkflowActionNames(input)
	want := []string{"Check style", "Test"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extractBuildBuddyWorkflowActionNames() = %v, want %v", got, want)
	}
}
