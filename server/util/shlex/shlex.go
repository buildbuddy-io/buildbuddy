// shlex contains facilities for shell code parsing and generation.

package shlex

import (
	"regexp"
	"strings"

	gshlex "github.com/google/shlex"
)

var (
	allSafeCharsRegexp   = regexp.MustCompile(`^[A-Za-z0-9/_\-]+$`)
	flagAssignmentRegexp = regexp.MustCompile(`^--[A-Za-z_-]+=`)
)

// Split parses the given shell command and returns the canonical tokenized
// arguments.
//
// Example:
//
//	shlex.Split("  foo --bar='/Quoted/Path/With Spaces'  ")
//	// Returns: []string{"foo", "--bar=/Quoted/Path/With Spaces"}
func Split(command string) ([]string, error) {
	return gshlex.Split(command)
}

// Quote escapes the given shell tokens so that each argument would be treated
// as verbatim elements of an arguments list.
//
// Example:
//
//	shlex.Quote("foo", "--path=has spaces", "quote's", "~")
//	// Returns: `foo --path='has spaces' 'quote'\''s' '~'`
//
// In this example, the first argument does not need to be escaped and can be
// rendered safely. The second argument has to be escaped because it has a
// space. Because it looks like a flag, we treat it specially by placing the
// quotes around the flag value (after the =). The third argument has a single
// quote and must be escaped to prevent the shell from warning about an unclosed
// delimiter. The fourth argument has a tilde which would be expanded to $HOME,
// so must also be escaped.
func Quote(tokens ...string) string {
	out := ""
	for i, arg := range tokens {
		out += quoteSingle(arg)
		if i < len(tokens)-1 {
			out += " "
		}
	}
	return out
}

func quoteSingle(arg string) string {
	if allSafeCharsRegexp.MatchString(arg) {
		return arg
	}
	// If we have a flag assignment like "--foo=bar baz" then keep the "--foo"
	// part unquoted so it renders more closely to how a human would enter it at
	// the command line.
	prefix := flagAssignmentRegexp.FindString(arg)
	suffix := strings.TrimPrefix(arg, prefix)
	return prefix + `'` + strings.ReplaceAll(suffix, `'`, `'\''`) + `'`
}
