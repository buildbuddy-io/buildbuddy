/*
Package terminal converts ANSI input to HTML output.

The generated HTML needs to be used with the stylesheet at
https://raw.githubusercontent.com/buildkite/terminal-to-html/master/assets/terminal.css
and wrapped in a term-container div.

Modified version of https://github.com/buildkite/terminal-to-html
*/
package terminal

import "bytes"

// RenderAsHTML converts ANSI to HTML and returns the result.
func RenderAsHTML(input []byte) []byte {
	screen := screen{}
	screen.parse(input)
	output := bytes.Replace(screen.asHTML(), []byte("\n\n"), []byte("\n&nbsp;\n"), -1)
	return output
}

// RenderAsANSI parses ANSI cursor codes, but retains color codes.
func RenderAsANSI(input []byte) []byte {
	screen := screen{}
	screen.parse(input)
	output := screen.asANSI()
	return output
}

// lol who named this thing :P
// ScreenWriter allows streaming data to a virtual screen
// that will be rendered after all writes are complete.
type ScreenWriter struct {
	s screen
}

func NewScreenWriter() *ScreenWriter {
	return &ScreenWriter{
		s: screen{},
	}
}

func (sw *ScreenWriter) Write(data []byte) (int, error) {
	sw.s.parse(data)
	return len(data), nil
}

func (sw *ScreenWriter) RenderAsANSI() []byte {
	return sw.s.asANSI()
}
