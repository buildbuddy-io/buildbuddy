package remotebazel

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type netrcCredentials struct {
	login    string
	password string
}

// netrcPath returns the path of the user's netrc file: $NETRC if set,
// otherwise ~/.netrc.
func netrcPath() (string, error) {
	if p := os.Getenv("NETRC"); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".netrc"), nil
}

// lookupNetrc returns the credentials in the user's netrc file for host. It
// returns nil if there is no netrc file or it has no entry for host.
func lookupNetrc(host string) (*netrcCredentials, error) {
	path, err := netrcPath()
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return parseNetrc(string(b), host), nil
}

// parseNetrc returns the credentials for host in netrc contents, falling back
// to the "default" entry. It returns nil if neither exists.
func parseNetrc(contents, host string) *netrcCredentials {
	var match, fallback *netrcCredentials
	// The entry that login/password tokens currently apply to, or nil if they
	// belong to an entry we don't care about.
	var current *netrcCredentials

	scanner := bufio.NewScanner(strings.NewReader(contents))
	inMacdef := false
	for scanner.Scan() {
		line := scanner.Text()
		if inMacdef {
			// A macro definition runs until the next blank line.
			if strings.TrimSpace(line) == "" {
				inMacdef = false
			}
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		fields := strings.Fields(line)
		for i := 0; i < len(fields); i++ {
			// Tokens other than "default" and "macdef" take a value.
			next := func() string {
				if i+1 >= len(fields) {
					return ""
				}
				i++
				return fields[i]
			}
			switch fields[i] {
			case "machine":
				current = nil
				if next() == host && match == nil {
					match = &netrcCredentials{}
					current = match
				}
			case "default":
				current = nil
				if fallback == nil {
					fallback = &netrcCredentials{}
					current = fallback
				}
			case "login":
				if v := next(); current != nil {
					current.login = v
				}
			case "password":
				if v := next(); current != nil {
					current.password = v
				}
			case "account":
				next()
			case "macdef":
				current = nil
				inMacdef = true
				i = len(fields)
			}
		}
	}
	if match != nil {
		return match
	}
	return fallback
}
