/*
Package terminal converts ANSI input to HTML output.

The generated HTML needs to be used with the stylesheet at
https://raw.githubusercontent.com/buildkite/terminal-to-html/master/assets/terminal.css
and wrapped in a term-container div.

Modified version of https://github.com/buildkite/terminal-to-html
*/
package terminal

const maxBuildLogLines = 10000

// lol who named this thing :P
// ScreenWriter allows streaming data to a virtual screen
// that will be rendered after all writes are complete.
type ScreenWriter struct {
	s *screen
	p *parser
}

func NewScreenWriter() *ScreenWriter {
	s := newScreen(maxBuildLogLines)
	s.style = &emptyStyle

	p := &parser{mode: MODE_NORMAL, screen: s}

	return &ScreenWriter{
		s: s,
		p: p,
	}
}

func (sw *ScreenWriter) Write(data []byte) (int, error) {
	sw.p.parseChunk(data)
	return len(data), nil
}

func (sw *ScreenWriter) RenderAsANSI() []byte {
	return sw.s.asANSI()
}

func (sw *ScreenWriter) PopExtraLinesAsANSI(linesToRetain int) []byte {
	return sw.s.popExtraLines(linesToRetain)
}
