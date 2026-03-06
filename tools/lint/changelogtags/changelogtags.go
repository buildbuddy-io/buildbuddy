package changelogtags

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	lintutil "github.com/buildbuddy-io/buildbuddy/tools/lint/util"
)

const changelogHeaderPath = "website/theme/ChangelogListPage/changelogHeader.tsx"

// Run checks that tags used in changelog entries are part of the supported tag set.
func Run(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	_ = stdout
	_ = fix

	filesToValidate, err := changelogFilesToValidate(files)
	if err != nil {
		return fmt.Errorf("determine changelog files to validate: %w", err)
	}
	if len(filesToValidate) == 0 {
		return nil
	}

	allowedTags, err := loadAllowedChangelogTags()
	if err != nil {
		return fmt.Errorf("load supported changelog tags: %w", err)
	}
	if len(allowedTags) == 0 {
		return fmt.Errorf("no supported changelog tags were loaded")
	}

	var validationErrors []string
	for _, file := range filesToValidate {
		if err := ctx.Err(); err != nil {
			return err
		}

		tags, err := parseTagsFromEntry(file)
		if err != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("%s: %s", file, err))
			continue
		}
		for _, tag := range tags {
			if !isAllowedChangelogTag(tag, allowedTags) {
				validationErrors = append(validationErrors, fmt.Sprintf("%s: unsupported tag %q", file, tag))
			}
		}
	}

	if len(validationErrors) == 0 {
		return nil
	}

	fmt.Fprintln(stderr, "unsupported changelog tags")
	fmt.Fprintf(stderr, "valid tags: %s\n", strings.Join(sortedTagSet(allowedTags), ", "))
	for _, e := range validationErrors {
		fmt.Fprintf(stderr, "- %s\n", e)
	}
	return fmt.Errorf("invalid change log tags")
}

func changelogFilesToValidate(files []string) ([]string, error) {
	// If the list of valid tags has changed, check that all changelog entries are still valid.
	if slices.Contains(files, changelogHeaderPath) {
		allMarkdownFiles, err := lintutil.GitListFilesWithExtensions([]string{".md", ".mdx"})
		if err != nil {
			return nil, fmt.Errorf("list all markdown files: %w", err)
		}
		files = allMarkdownFiles
	}

	filtered := make([]string, 0, len(files))
	for _, file := range files {
		if !strings.HasPrefix(file, "website/changelog/") {
			continue
		}
		ext := filepath.Ext(file)
		if ext != ".md" && ext != ".mdx" {
			continue
		}
		filtered = append(filtered, file)
	}
	return filtered, nil
}

func loadAllowedChangelogTags() (map[string]struct{}, error) {
	content, err := os.ReadFile(changelogHeaderPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", changelogHeaderPath, err)
	}

	// Keep in sync with CHANGELOG_TAGS entries in changelogHeader.tsx.
	re := regexp.MustCompile(`\{\s*label:\s*"([^"]+)"\s*,\s*url:\s*"([^"]+)"\s*\}`)
	matches := re.FindAllStringSubmatch(string(content), -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("no changelog tags found in %s", changelogHeaderPath)
	}

	allowed := make(map[string]struct{})
	for _, m := range matches {
		label := normalizeTagToken(m[1])

		if label != "" {
			allowed[label] = struct{}{}
		}
	}
	return allowed, nil
}

func parseTagsFromEntry(path string) ([]string, error) {
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
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "tags:") {
			continue
		}

		value := strings.TrimSpace(strings.TrimPrefix(trimmed, "tags:"))
		switch {
		case value == "":
			var tags []string
			for j := i + 1; j < len(lines); j++ {
				raw := lines[j]
				item := strings.TrimSpace(raw)
				if item == "" {
					continue
				}
				// End of list if not a list item.
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

func isAllowedChangelogTag(tag string, allowed map[string]struct{}) bool {
	for t := range allowed {
		if t == tag {
			return true
		}
	}
	return false
}

func cleanTagValue(v string) string {
	v = strings.TrimSpace(v)
	v = strings.Trim(v, `"'`)
	return strings.TrimSpace(v)
}

func normalizeTagToken(v string) string {
	return strings.ToLower(cleanTagValue(v))
}

func sortedTagSet(tags map[string]struct{}) []string {
	list := make([]string, 0, len(tags))
	for tag := range tags {
		list = append(list, tag)
	}
	slices.Sort(list)
	return list
}
