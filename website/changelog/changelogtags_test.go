package changelog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"gopkg.in/yaml.v3"
)

// Set via x_defs in BUILD file.
var changelogTagsRlocationpath string

func TestChangelogTagsAreSupported(t *testing.T) {
	tagsPath, err := runfiles.Rlocation(changelogTagsRlocationpath)
	if err != nil {
		t.Fatalf("locate changelog-tags.json: %s", err)
	}

	allowedTags, err := loadAllowedChangelogTags(tagsPath)
	if err != nil {
		t.Fatalf("load supported changelog tags: %s", err)
	}

	root := filepath.Clean(filepath.Join(filepath.Dir(tagsPath), "../.."))
	changelogPaths, err := filepath.Glob(filepath.Join(root, "website/changelog/*.md"))
	if err != nil {
		t.Fatalf("list changelog entries: %s", err)
	}
	mdxPaths, err := filepath.Glob(filepath.Join(root, "website/changelog/*.mdx"))
	if err != nil {
		t.Fatalf("list changelog entries: %s", err)
	}
	changelogPaths = append(changelogPaths, mdxPaths...)

	var validationErrors []string
	for _, path := range changelogPaths {
		tags, err := parseChangelogFrontmatterTags(path)
		if err != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("%s: %s", rel(root, path), err))
			continue
		}
		for _, tag := range tags {
			if _, ok := allowedTags[tag]; !ok {
				validationErrors = append(validationErrors, fmt.Sprintf("%s: unsupported tag %q", rel(root, path), tag))
			}
		}
	}

	if len(validationErrors) == 0 {
		return
	}

	var b strings.Builder
	b.WriteString("unsupported changelog tag(s) found\n")
	b.WriteString(fmt.Sprintf("valid tags: %s\n", strings.Join(sortedTagSet(allowedTags), ", ")))
	for _, e := range validationErrors {
		b.WriteString("- ")
		b.WriteString(e)
		b.WriteString("\n")
	}
	t.Fatal(strings.TrimSuffix(b.String(), "\n"))
}

type changelogTag struct {
	Label string `json:"label"`
	URL   string `json:"url"`
}

func loadAllowedChangelogTags(tagsPath string) (map[string]struct{}, error) {
	content, err := os.ReadFile(tagsPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", tagsPath, err)
	}

	var tags []changelogTag
	if err := json.Unmarshal(content, &tags); err != nil {
		return nil, fmt.Errorf("parse %s: %w", tagsPath, err)
	}
	if len(tags) == 0 {
		return nil, fmt.Errorf("no changelog tags found in %s", tagsPath)
	}

	allowed := make(map[string]struct{})
	for _, tag := range tags {
		if tag.Label != "" {
			allowed[tag.Label] = struct{}{}
		}
	}
	return allowed, nil
}

type changelogFrontmatter struct {
	Tags []string `yaml:"tags"`
}

func parseChangelogFrontmatterTags(path string) ([]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	frontmatter, ok := extractFrontmatter(string(content))
	if !ok {
		return nil, nil
	}
	var fm changelogFrontmatter
	if err := yaml.Unmarshal([]byte(frontmatter), &fm); err != nil {
		return nil, fmt.Errorf("parse frontmatter: %w", err)
	}
	return fm.Tags, nil
}

func extractFrontmatter(content string) (string, bool) {
	lines := strings.Split(content, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return "", false
	}
	for i := 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "---" {
			return strings.Join(lines[1:i], "\n"), true
		}
	}
	return "", false
}

func sortedTagSet(tags map[string]struct{}) []string {
	list := make([]string, 0, len(tags))
	for tag := range tags {
		list = append(list, tag)
	}
	slices.Sort(list)
	return list
}

func rel(root, path string) string {
	r, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return r
}
