package terminal

import (
	bkterminal "github.com/buildkite/terminal-to-html/v3"
)

type Writer struct {
	screen *bkterminal.Screen
}

func NewWriter() *Writer {
	return &Writer{screen: bkterminal.NewScreen()}
}

func (w *Writer) Write(p []byte) (int, error) {
	bkterminal.ParseANSIToScreen(w.screen, p)
	return len(p), nil
}

func (w *Writer) Render() []byte {
	return w.screen.AsANSI()
}

func (w *Writer) PopExtraLines(numLinesToRetain int) []byte {
	return w.screen.FlushLinesFromTop(numLinesToRetain)
}
