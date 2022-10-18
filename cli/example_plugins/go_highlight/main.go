package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/logrusorgru/aurora/v4"
)

var (
	goSrcPattern = regexp.MustCompile(`^(.*?\.go:\d+:\d+:)(.*)`)
)

func main() {
	if err := run(); err != nil {
		io.WriteString(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}
}

func run() error {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(splitLinesPreservingNewline)
	for scanner.Scan() {
		text := scanner.Text()
		if m := goSrcPattern.FindStringSubmatch(text); m != nil {
			text = fmt.Sprintf("%s%s", aurora.Yellow(m[1]), m[2])
		}
		if _, err := io.WriteString(os.Stdout, text); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// splitLinesPreservingNewline is a split function meant to be used with
// bufio.Scanner. Unlike the default split function, it preserves the trailing
// newline if it exists, so that the caller can tell whether the final line of
// input has a newline or not.
func splitLinesPreservingNewline(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[0 : i+1], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}
