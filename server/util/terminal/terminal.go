package terminal

import (
	bkterminal "github.com/buildkite/terminal-to-html/v3"
)

type ScreenWriter struct {
	screen *bkterminal.Screen
}

func NewScreenWriter() *ScreenWriter {
	return &ScreenWriter{screen: bkterminal.NewScreen()}
}

func (w *ScreenWriter) Write(p []byte) (int, error) {
	bkterminal.ParseANSIToScreen(w.screen, p)
	return len(p), nil
}

func (w *ScreenWriter) Render() []byte {
	return w.screen.AsANSI()
}

func (w *ScreenWriter) PopExtraLines(numLinesToRetain int) []byte {
	return w.screen.FlushLinesFromTop(numLinesToRetain)
}
