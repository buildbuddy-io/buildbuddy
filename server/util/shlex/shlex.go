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

// Quote returns a string that can be safely used with the shell command
// line.
//
// Shell commands are frequently passed as a slice of strings. Quote escapes
// the input tokens, so the output string can safely be parsed back into a slice
// of strings using `Split`.
//
// Example:
//
//		s := shlex.Quote("foo", "--path=has spaces", "quote's", "~")
//		// Returns: `foo --path='has spaces' 'quote'\''s' '~'`
//	 tokens := shlex.Split(s)
//	 // Returns: ["foo", "--path=has spaces", "quote's", "~"]
//
// In this example, the first argument "foo" does not need to be escaped and can be
// rendered safely.
// The second argument "--path=has spaces" has to be escaped because it has a
// space. Because it looks like a flag, we treat it specially by placing the
// quotes around the flag value (after the =).
// The third argument "quote's" has a single quote and must be escaped to prevent
// the shell from warning about an unclosed delimiter.
// The fourth argument "~" has a tilde which would be expanded to $HOME,
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
