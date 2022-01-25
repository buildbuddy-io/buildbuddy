package parser

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	bblog "github.com/buildbuddy-io/buildbuddy/cli/logging"
)

var (
	optionMatcher = regexp.MustCompile("(?P<phase>[^:]*):?(?P<config>.*)?\\s+(?P<option>.*)")
	importMatcher = regexp.MustCompile("(import|try-import)\\s+(?P<relative>\\%workspace\\%)?(?P<path>.*)")
)

type BazelOption struct {
	Phase  string
	Config string
	Option string
}

func appendOptionsFromImport(match []string, opts []*BazelOption) ([]*BazelOption, error) {
	importPath := ""
	for i, name := range importMatcher.SubexpNames() {
		switch name {
		case "relative":
			if len(match[i]) > 0 {
				importPath, _ = os.Getwd()
			}
		case "path":
			importPath = filepath.Join(importPath, match[i])
		}
	}
	file, err := os.Open(importPath)
	if err != nil {
		return opts, err
	}
	defer file.Close()
	return appendOptionsFromFile(file, opts)
}

func optionFromMatch(match []string) *BazelOption {
	o := &BazelOption{}
	for i, name := range optionMatcher.SubexpNames() {
		switch name {
		case "phase":
			o.Phase = match[i]
		case "config":
			o.Config = match[i]
		case "option":
			o.Option = match[i]
		}
	}
	return o
}

func appendOptionsFromFile(in io.Reader, opts []*BazelOption) ([]*BazelOption, error) {
	scanner := bufio.NewScanner(in)
	var err error
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}

		if importMatcher.MatchString(line) {
			match := importMatcher.FindStringSubmatch(line)
			opts, err = appendOptionsFromImport(match, opts)
			if err != nil {
				bblog.Printf("Error parsing import: %s", err.Error())
			}
			continue
		}

		if optionMatcher.MatchString(line) {
			match := optionMatcher.FindStringSubmatch(line)
			opts = append(opts, optionFromMatch(match))
		}
	}
	return opts, scanner.Err()
}

func ParseRCFiles(filePaths ...string) ([]*BazelOption, error) {
	options := make([]*BazelOption, 0)
	for _, filePath := range filePaths {
		file, err := os.Open(filePath)
		if err != nil {
			continue
		}
		defer file.Close()
		options, err = appendOptionsFromFile(file, options)
		if err != nil {
			continue
		}
	}
	return options, nil
}

func GetFlagValue(options []*BazelOption, phase, config, flagName, commandLineOverride string) string {
	if commandLineOverride != "" {
		return flagName + "=" + commandLineOverride
	}
	for _, opt := range options {
		if opt.Phase != phase {
			continue
		}
		if opt.Config != config && opt.Config != "" {
			continue
		}
		if strings.HasPrefix(opt.Option, flagName) {
			return opt.Option
		}
	}
	return ""
}
