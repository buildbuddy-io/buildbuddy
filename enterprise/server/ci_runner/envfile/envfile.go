// Package envfile manages the env file, which lets workflow steps persist
// environment variables to subsequent steps by appending to the file that
// $BUILDBUDDY_ENV points to.
package envfile

import (
	"fmt"
	"os"
	"strings"
)

const (
	// FileName is the name of the env file.
	FileName = "buildbuddy.env"
	// EnvVarName is the name of the environment variable holding the env
	// file path.
	EnvVarName = "BUILDBUDDY_ENV"
)

// Provision creates an empty env file at the given path and sets the
// BUILDBUDDY_ENV environment variable to point to it. This should be called
// once at the start of an action, before any steps run.
func Provision(path string) error {
	// N.B. os.WriteFile overwrites path by default if it already exists.
	if err := os.WriteFile(path, nil, 0644); err != nil {
		return fmt.Errorf("write %q: %w", path, err)
	}
	return os.Setenv(EnvVarName, path)
}

// ParseAndReset parses the env file at the given path and returns a map of
// environment variables. After parsing, the file is truncated to prepare for
// the next step.
func ParseAndReset(path string) (map[string]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	envVars, err := Parse(string(content))
	if err != nil {
		return nil, fmt.Errorf("parse $%s: %w", EnvVarName, err)
	}
	// Truncate the file for the next step
	if err := os.Truncate(path, 0); err != nil {
		return nil, err
	}
	return envVars, nil
}

// Parse parses env file content and returns a map of environment variables.
// It follows the same syntax and semantics as GitHub's GITHUB_ENV file:
// - Single-line values: NAME=value
// - Multiline values (heredoc syntax):
//
//	NAME<<DELIMITER
//	line1
//	line2
//	DELIMITER
//
// If a line contains both "=" and "<<", whichever appears first determines
// the format. Names and delimiters are not trimmed, and the closing delimiter
// line must match the delimiter exactly.
func Parse(content string) (map[string]string, error) {
	envVars := make(map[string]string)
	lines := strings.Split(content, "\n")

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		// Skip empty lines
		if line == "" {
			continue
		}

		equalsIndex := strings.Index(line, "=")
		heredocIndex := strings.Index(line, "<<")

		// Single-line format: NAME=value
		if equalsIndex >= 0 && (heredocIndex < 0 || equalsIndex < heredocIndex) {
			name := line[:equalsIndex]
			value := line[equalsIndex+1:]
			if name == "" {
				return nil, fmt.Errorf("line %d: invalid format %q: variable name must not be empty", i+1, line)
			}
			envVars[name] = value
			continue
		}

		// Multiline format: NAME<<DELIMITER
		if heredocIndex >= 0 {
			name := line[:heredocIndex]
			delimiter := line[heredocIndex+2:]
			if name == "" || delimiter == "" {
				return nil, fmt.Errorf("line %d: invalid format %q: variable name and delimiter must not be empty", i+1, line)
			}

			// Collect lines verbatim until a line exactly matching the
			// delimiter is found.
			var valueLines []string
			startLine := i + 1 // 1-indexed
			i++
			found := false
			for ; i < len(lines); i++ {
				if lines[i] == delimiter {
					found = true
					break
				}
				valueLines = append(valueLines, lines[i])
			}
			if !found {
				return nil, fmt.Errorf("line %d: matching delimiter %q not found for variable %q", startLine, delimiter, name)
			}
			envVars[name] = strings.Join(valueLines, "\n")
			continue
		}

		return nil, fmt.Errorf("line %d: invalid format %q: expected NAME=value or NAME<<DELIMITER", i+1, line)
	}

	return envVars, nil
}
