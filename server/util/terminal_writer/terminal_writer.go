package terminal_writer

import (
	"github.com/buildkite/terminal-to-html/v3"
)

type Writer struct {
	screen *terminal.Screen
}

func NewWriter() *Writer {
	return &Writer{screen: terminal.NewScreen()}
}

func (w *Writer) Write(p []byte) (int, error) {
	terminal.ParseANSIToScreen(w.screen, p)
	return len(p), nil
}

func (w *Writer) Render() []byte {
	return w.screen.AsANSI()
}

func (w *Writer) PopExtraLines(numLinesToRetain int) []byte {
	return w.screen.FlushLinesFromTop(numLinesToRetain)
}
