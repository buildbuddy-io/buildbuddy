package terminal

import (
	bkterminal "github.com/buildkite/terminal-to-html/v3"
)

type ScreenWriter struct {
	*bkterminal.Screen
}

func NewScreenWriter() (*ScreenWriter, error) {
	s, err := bkterminal.NewScreen()
	if err != nil {
		return nil, err
	}
	return &ScreenWriter{Screen: s}, nil
}

func (w *ScreenWriter) Render() string {
	return w.Screen.AsANSI()
}

func (w *ScreenWriter) PopExtraLines(numLinesToRetain int) string {
	return w.Screen.FlushLinesFromTop(numLinesToRetain)
}
