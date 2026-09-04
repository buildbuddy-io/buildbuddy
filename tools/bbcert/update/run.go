package update

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// NoUpdateEnv can be used to disable the automatic update check.
const NoUpdateEnv = "BBCERT_NO_UPDATE"

// Check fetches the published manifest and reports whether it names a
// different build than this one.
func (u *Updater) Check(ctx context.Context) (m *Manifest, needed bool, err error) {
	m, err = u.Latest(ctx)
	if err != nil {
		return nil, false, err
	}
	return m, m.Commit != Commit(), nil
}

// Run implements `bbcert update`.
func Run(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("bbcert update", flag.ContinueOnError)
	commit := fs.String("commit", "", "Install this published commit instead of what latest points at.")
	check := fs.Bool("check", false, "Report what is published without installing it.")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "unexpected argument %q\n", fs.Arg(0))
		fs.Usage()
		return 2
	}

	u, err := Default()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var m *Manifest
	if *commit != "" {
		m, err = u.ForCommit(ctx, *commit)
	} else {
		m, err = u.Latest(ctx)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	current := Commit()
	fmt.Printf("Running:   %s\n", orUnstamped(current))
	fmt.Printf("Published: %s (%s)\n", m.Commit, m.PublishedAt)
	if *check {
		return 0
	}
	if m.Commit == current {
		fmt.Println("Already up to date.")
		return 0
	}
	if err := u.Apply(ctx, m); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("Installed %s.\n", short(m.Commit))
	return 0
}

func short(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}

func orUnstamped(commit string) string {
	if commit == "" {
		return "unstamped development build"
	}
	return commit
}
