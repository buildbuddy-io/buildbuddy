package changelog

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
)

func TestChangelogTagsAreSupported(t *testing.T) {
	root, tagsPath := workspaceRootAndTagsPath(t)

	allowedTags, err := loadAllowedChangelogTags(tagsPath)
	if err != nil {
		t.Fatalf("load supported changelog tags: %s", err)
	}

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

func workspaceRootAndTagsPath(t *testing.T) (root, tagsPath string) {
	t.Helper()
	const relTagsPath = "website/changelog/changelog-tags.json"

	if wd := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); wd != "" {
		p := filepath.Join(wd, relTagsPath)
		if _, err := os.Stat(p); err == nil {
			return wd, p
		}
	}

	candidates := []string{}
	if ws := os.Getenv("TEST_WORKSPACE"); ws != "" {
		candidates = append(candidates, ws)
	}
	candidates = append(candidates, "_main", "__main__", "buildbuddy")

	for _, ws := range candidates {
		runfile := path.Join(ws, relTagsPath)
		p, err := runfiles.Rlocation(runfile)
		if err != nil {
			continue
		}
		// changelog-tags.json lives at <workspace_root>/website/changelog/changelog-tags.json
		root = filepath.Clean(filepath.Join(filepath.Dir(p), "../.."))
		return root, p
	}

	if _, file, _, ok := runtime.Caller(0); ok {
		root = filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
		p := filepath.Join(root, relTagsPath)
		if _, err := os.Stat(p); err == nil {
			return root, p
		}
	}

	t.Fatalf("could not locate changelog-tags.json via runfiles or local source path (TEST_WORKSPACE=%q)", os.Getenv("TEST_WORKSPACE"))
	return "", ""
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

func parseChangelogFrontmatterTags(path string) ([]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	frontmatter, ok := extractFrontmatter(string(content))
	if !ok {
		return nil, nil
	}
	return parseTagsFromFrontmatter(frontmatter)
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

func parseTagsFromFrontmatter(frontmatter string) ([]string, error) {
	lines := strings.Split(frontmatter, "\n")
	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trimmed, "tags:") {
			continue
		}

		value := strings.TrimSpace(strings.TrimPrefix(trimmed, "tags:"))
		switch {
		case value == "":
			var tags []string
			for j := i + 1; j < len(lines); j++ {
				item := strings.TrimSpace(lines[j])
				if item == "" {
					continue
				}
				if !strings.HasPrefix(item, "-") {
					break
				}
				item = strings.TrimSpace(strings.TrimPrefix(item, "-"))
				if item == "" || strings.Contains(item, ":") {
					return nil, fmt.Errorf("unsupported tags format in frontmatter")
				}
				tags = append(tags, cleanTagValue(item))
			}
			return tags, nil
		case strings.HasPrefix(value, "["):
			closing := strings.LastIndex(value, "]")
			if closing == -1 {
				return nil, fmt.Errorf("malformed inline tags list in frontmatter")
			}
			inner := value[1:closing]
			if strings.TrimSpace(inner) == "" {
				return nil, nil
			}
			parts := strings.Split(inner, ",")
			tags := make([]string, 0, len(parts))
			for _, part := range parts {
				parsed := cleanTagValue(part)
				if parsed != "" {
					tags = append(tags, parsed)
				}
			}
			return tags, nil
		default:
			if strings.Contains(value, ":") {
				return nil, fmt.Errorf("unsupported tags format in frontmatter")
			}
			return []string{cleanTagValue(value)}, nil
		}
	}
	return nil, nil
}

func cleanTagValue(v string) string {
	v = strings.TrimSpace(v)
	v = strings.Trim(v, `"'`)
	return strings.TrimSpace(v)
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
