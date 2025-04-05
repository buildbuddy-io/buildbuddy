package terminal

import (
	"strings"

	bkterminal "github.com/buildkite/terminal-to-html/v3"
)

type ScreenWriter struct {
	*bkterminal.Screen
	ScrollOut         strings.Builder
	ScrollOutWriteErr error
}

// NewScreenWriter returns a ScreenWriter backed by an ANSI state machine with a
// window size of windowHeight. Any lines that scroll off the top are written to
// the ScrollOut string builder, and any write errors encountered during that
// process are recorded in ScrollOutWriteErr.
// A windowHeight of less than 1 indicates a window of unlimited size.
func NewScreenWriter(windowHeight int) (*ScreenWriter, error) {
	s, err := bkterminal.NewScreen(bkterminal.WithMaxSize(0, windowHeight), bkterminal.WithANSIRenderer())
	if err != nil {
		return nil, err
	}
	w := &ScreenWriter{Screen: s}
	if windowHeight > 0 {
		s.ScrollOutFunc = func(line string) { _, w.ScrollOutWriteErr = w.ScrollOut.WriteString(line) }
	}
	return w, nil
}

func (w *ScreenWriter) Render() string {
	s, _ := w.Screen.AsANSI()
	return s
}

func (w *ScreenWriter) Reset(windowHeight int) error {
	w.ScrollOut.Reset()
	var err error
	w.Screen, err = bkterminal.NewScreen(bkterminal.WithMaxSize(0, windowHeight), bkterminal.WithANSIRenderer())
	return err
}
