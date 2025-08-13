package terminal

import (
	"strings"

	bkterminal "github.com/buildkite/terminal-to-html/v3"
)

const (
	// The number of columns to use for the terminal-to-html ANSI state machine.
	// All lines that fit in this number of columns (characters) will not wrap to
	// the next line.
	// We cannot make this number arbitrarily large because the state machine uses
	// it as the `cap` parameter when making slices to represent lines, which
	// means we are actually allocating space for slices of this length.
	// TODO: patch the library to make the cap for slices and the max columns two
	// different numbers, ideally as a PR.
	MaxColumnCapacity     = 2000
	DefaultColumnCapacity = 160
	MaxLineCapacity       = 32
)

type ScreenWriter struct {
	*bkterminal.Screen
	OutputAccumulator strings.Builder
	WriteErr          error
	windowWidth       int
	windowHeight      int
	renderer          *bkterminal.ANSIRenderer
}

// NewScreenWriter returns a ScreenWriter backed by an ANSI state machine with a
// window size of requestedWindowWidth by requestedWindowHeight, each dimension
// capped by MaxColumnCapacity and MaxLineCapacity, respectively. Any lines that
// scroll off the top are written to the ScrollOut string builder, and any write
// errors encountered during that process are recorded in ScrollOutWriteErr.
// A windowWidth of less than 1 indicates a window of maximum width.
// A requestedWindowHeight of less than 1 indicates a window of maximum height
// with no scroll-out.
func NewScreenWriter(requestedWindowWidth int, requestedWindowHeight int) (*ScreenWriter, error) {
	w := &ScreenWriter{
		windowWidth:  min(requestedWindowWidth, MaxColumnCapacity),
		windowHeight: min(requestedWindowHeight, MaxLineCapacity),
		renderer:     &bkterminal.ANSIRenderer{},
	}
	if w.windowWidth == 0 {
		w.windowWidth = MaxLineCapacity
	}
	if w.windowHeight == 0 {
		w.windowHeight = MaxLineCapacity
	}
	s, err := bkterminal.NewScreen(
		bkterminal.WithSize(MaxColumnCapacity, w.windowHeight),
		bkterminal.WithRenderer(w.renderer),
		bkterminal.WithRealWindow(),
		bkterminal.WithDefaultColumnCapacity(min(w.windowWidth, DefaultColumnCapacity)),
	)
	if err != nil {
		return nil, err
	}
	w.Screen = s
	if requestedWindowHeight > 0 {
		s.ScrollOutFunc = func(line string) { _, w.WriteErr = w.OutputAccumulator.WriteString(line) }
	}
	return w, nil
}

func (w *ScreenWriter) Render() string {
	s, _ := w.Screen.AsANSI(w.renderer.Style())
	return s
}

func (w *ScreenWriter) Reset() error {
	w.OutputAccumulator.Reset()
	w.renderer = &bkterminal.ANSIRenderer{}
	var err error
	w.Screen, err = bkterminal.NewScreen(
		bkterminal.WithSize(MaxColumnCapacity, w.windowHeight),
		bkterminal.WithRenderer(w.renderer),
		bkterminal.WithRealWindow(),
		bkterminal.WithDefaultColumnCapacity(min(w.windowWidth, DefaultColumnCapacity)),
	)
	return err
}

func (w *ScreenWriter) AccumulatingOutput() bool {
	// If there's no ScrollOutFunc, then there's no window giving us scrollOut.
	return w.ScrollOutFunc != nil
}
