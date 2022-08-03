package makedb

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type parseState string

const (
	parseInit             parseState = "init"
	parseVariables        parseState = "parseVariables"
	parseImplicitRules    parseState = "parseImplicitRules"
	parseFiles            parseState = "parseFiles"
	parseVPATHSearchPaths parseState = "parseVPATHSearchPaths"
)

var (
	variableDefinitionRE = regexp.MustCompile(`^(.*?)\s*(:?=)\s*(.*)$`)
	recipeInstructionsRE = regexp.MustCompile(`^\s+(.*)$`)
)

type Variable struct {
	// Type is the variable type: "environment", "automatic", "default", etc.
	Type string
	// Name is the variable name.
	Name string
	// Value is the variable value.
	Value string
	// DefType is either "=" or ":=".
	DefType string
}

// File is a structured representation of Make's notion of a "File".
type File struct {
	Name         string
	Dependencies []string
	// Recipe contains the commands needed to build the file. Leading whitespace
	// is stripped. Multiple commands may be separated by newlines.
	Recipe   string
	IsTarget bool
}

// Dir returns the dir part of the file name, including a trailing "/".
func (f *File) Dir() string {
	return f.Name[:len(f.Name)-len(f.Base())]
}

// Base returns the basename of the file name.
func (f *File) Base() string {
	return filepath.Base(f.Name)
}

// DB holds the structured output of `make --print-data-base`.
type DB struct {
	Variables     []*Variable
	Files         []*File
	ImplicitRules []*File

	// Indexes

	VariablesByName map[string]*Variable
	FilesByName     map[string]*File
}

func Parse(b []byte) (*DB, error) {
	db := &DB{}
	state := parseInit
	lines := strings.Split(string(b), "\n")
	var curVariable *Variable
	var curFile *File
	for i, line := range lines {
		lineNumber := i + 1
		if state == parseInit {
			if line == "# Variables" {
				state = parseVariables
				continue
			}
			continue
		}

		if state == parseVariables {
			// TODO: Don't ignore the following sections:
			// # Pattern-specific Variable Values
			// # Directories
			if line == "# Implicit Rules" {
				state = parseImplicitRules
				continue
			}

			if strings.HasPrefix(line, "# ") {
				curVariable = &Variable{Type: strings.TrimPrefix(line, "# ")}
				db.Variables = append(db.Variables, curVariable)
				continue
			}
			if m := variableDefinitionRE.FindStringSubmatch(line); m != nil {
				if curVariable == nil {
					return nil, status.UnknownErrorf("invalid parse state on line %d: variable definition without preceding comment", lineNumber)
				}
				curVariable.Name = m[1]
				curVariable.DefType = m[2]
				curVariable.Value = m[3]
				curVariable = nil
				continue
			}
		}

		if state == parseImplicitRules {
			if line == "# Files" {
				curFile = nil
				state = parseFiles
				continue
			}
			if strings.HasPrefix(line, "#") {
				continue
			}
			if m := recipeInstructionsRE.FindStringSubmatch(line); m != nil {
				if curFile == nil {
					return nil, status.UnknownErrorf("invalid parse state on line %d: recipe instruction without preceding target specification", lineNumber)
				}
				instruction := m[1]
				curFile.Recipe += instruction + "\n"
			} else if strings.Contains(line, ":") {
				parts := strings.Split(line, ":")
				deps := strings.Fields(parts[1])
				curFile = &File{
					Name:         parts[0],
					Dependencies: deps,
				}
				db.ImplicitRules = append(db.ImplicitRules, curFile)
			}
		}

		if state == parseFiles {
			if line == "# VPATH Search Paths" {
				curFile = nil
				state = parseVPATHSearchPaths
				continue
			}
			if strings.HasPrefix(line, "#") {
				continue
			}
			if m := recipeInstructionsRE.FindStringSubmatch(line); m != nil {
				if curFile == nil {
					return nil, status.UnknownErrorf("invalid parse state on line %d: recipe instruction without preceding target specification", lineNumber)
				}
				instruction := m[1]
				curFile.Recipe += instruction + "\n"
			} else if strings.Contains(line, ":") {
				parts := strings.Split(line, ":")
				deps := strings.Fields(parts[1])
				curFile = &File{
					IsTarget:     lines[i-1] != "# Not a target:",
					Name:         parts[0],
					Dependencies: deps,
				}
				db.Files = append(db.Files, curFile)
			}
		}
	}

	db.buildIndexes()
	return db, nil
}

// ExpandImplicitRules traces the dependency chain of `targets` and if any
// targets in the graph do not have recipes, the implicit rule search algorithm
// is run in order to find a generating rule.
// https://www.gnu.org/software/make/manual/html_node/Implicit-Rule-Search.html
func (db *DB) ExpandImplicitRules(dir string, targets []string) error {
	e := map[string]bool{}
	exists := func(name string) bool {
		ex, ok := e[name]
		if ok {
			return ex
		}
		_, err := os.Stat(filepath.Join(dir, name))
		// TODO: Handle non-`os.IsNotExist` errors
		ex = err == nil
		e[name] = ex
		return ex
	}
	oughtToExist := map[string]bool{}
	for _, f := range db.Files {
		if f.IsTarget {
			oughtToExist[f.Name] = true
			continue
		}
		for _, p := range f.Dependencies {
			oughtToExist[p] = true
		}
	}

	// TODO: More parallelism here

	visited := map[string]struct{}{}
	frontier := append([]string{}, targets...)
	for len(frontier) > 0 {
		t := frontier[0]
		frontier = frontier[1:]
		if _, ok := visited[t]; ok {
			continue
		}
		visited[t] = struct{}{}
		f := db.FilesByName[t]
		if f != nil {
			// Expand implicit rules for all dependencies.
			frontier = append(frontier, f.Dependencies...)
			if f.Recipe != "" {
				// We have an explicit recipe for the target; do nothing.
				continue
			}
		}
		// Look for implicit rules:
		// https://www.gnu.org/software/make/manual/html_node/Implicit-Rule-Search.html
		// Step 1:
		_, n := splitTarget(t)
		// Step 2:
		type match struct {
			rule *File
			s    string
		}
		var matches []*match
		for _, rule := range db.ImplicitRules {
			pattern := rule.Name
			targetPart := n
			if strings.Contains(pattern, "/") {
				targetPart = t
			}
			// Step 4 is applied here (remove rules with no recipe)
			if s := matchPattern(targetPart, pattern); s != "" && rule.Recipe != "" {
				matches = append(matches, &match{rule: rule, s: s})
			}
		}
		// fmt.Printf("> potential matches (after step 2): %+v\n", spew.Sdump(matches))
		// TODO: Step 3 ("match-anything" handling)
		// Step 5:
		// First compute the set of labels which "exist or ought to exist"
		var found *match
		for _, m := range matches {
			applies := true
			for _, p := range m.rule.Dependencies {
				p = strings.ReplaceAll(p, "%", m.s)
				if !exists(p) && !oughtToExist[p] {
					applies = false
					break
				}
			}
			if applies {
				found = m
				break
			}
		}
		if found == nil {
			return status.InvalidArgumentErrorf("could not find rule for target %q", t)
		}

		// Create a concrete File for the match.
		{
			f := &File{
				Name:     strings.ReplaceAll(found.rule.Name, "%", found.s),
				Recipe:   found.rule.Recipe,
				IsTarget: true,
			}
			f.Dependencies = make([]string, 0, len(found.rule.Dependencies))
			for _, p := range found.rule.Dependencies {
				p = strings.ReplaceAll(p, "%", found.s)
				f.Dependencies = append(f.Dependencies, p)
				if _, ok := db.FilesByName[p]; !ok {
					db.addFile(&File{Name: p})
				}
			}
			db.addFile(f)
		}

		// Step 6 (try harder - apply the algorithm recursively to see if
		// prerequisites can be generated by implicit rules).
		// TODO!
	}
	return nil
}

func (db *DB) addFile(f *File) {
	db.Files = append(db.Files, f)
	db.FilesByName[f.Name] = f
}

// buildIndexes builds up the convenience mappings for getting DB objects by
// name.
func (db *DB) buildIndexes() {
	db.FilesByName = make(map[string]*File, len(db.Files))
	for _, f := range db.Files {
		db.FilesByName[f.Name] = f
	}

	db.VariablesByName = make(map[string]*Variable, len(db.Variables))
	for _, v := range db.Variables {
		db.VariablesByName[v.Name] = v
	}
}

func splitTarget(t string) (d, n string) {
	n = filepath.Base(t)
	d = t[len(t)-len(n):]
	return d, n
}

func matchPattern(target, pattern string) (stem string) {
	pattern = regexp.QuoteMeta(pattern)
	pattern = strings.ReplaceAll(pattern, "%", "(.+?)")
	// TODO: Cache these for better perf
	re := regexp.MustCompile(pattern)
	m := re.FindStringSubmatch(target)
	if len(m) <= 1 {
		return ""
	}
	return m[1]
}
