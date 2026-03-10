package markdown

import (
	"bytes"
	"io"

	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
)

var (
	// H1Color is the ANSI color style applied to markdown H1 headings.
	H1Color = terminal.Esc(1, 36)
	// H2Color is the ANSI color style applied to markdown H2 headings.
	H2Color = terminal.Esc(36)
	// ResetColor is the ANSI reset style applied after colored headings.
	ResetColor = terminal.Esc()
)

// ColorScheme configures heading colors applied by Writer.
type ColorScheme struct {
	H1Color string
	H2Color string
	Reset   string
}

// Options configures Writer behavior.
type Options struct {
	// ColorScheme overrides heading styles. If nil, Writer uses package defaults.
	ColorScheme *ColorScheme
}

// StreamingWriter is an io.Writer that accumulates write failures and exposes
// them via Err.
type StreamingWriter interface {
	io.Writer
	Err() error
}

// Writer returns an io.Writer that colorizes streamed markdown headings.
//
// If opts is nil or opts.ColorScheme is nil, Writer uses package defaults.
func Writer(w io.Writer, opts *Options) StreamingWriter {
	scheme := defaultColorScheme()
	if opts != nil && opts.ColorScheme != nil {
		scheme = *opts.ColorScheme
		if scheme.Reset == "" {
			scheme.Reset = ResetColor
		}
	}
	return &writer{
		w:           w,
		atLineStart: true,
		scheme:      scheme,
	}
}

type writer struct {
	w                  io.Writer
	atLineStart        bool
	inCodeFence        bool
	linePrefix         []byte
	prefixKind         prefixKind
	activeHeadingColor string
	scheme             ColorScheme
	err                error
}

type prefixKind int

const (
	prefixNone prefixKind = iota
	prefixHash
	prefixBacktick
)

func (w *writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return len(p), nil
	}

	var out bytes.Buffer

	for _, b := range p {
		if w.activeHeadingColor != "" {
			if b == '\n' {
				out.WriteString(w.scheme.Reset)
				out.WriteByte('\n')
				w.activeHeadingColor = ""
				w.atLineStart = true
				continue
			}
			out.WriteByte(b)
			continue
		}

		if w.atLineStart {
			w.writeAtLineStart(&out, b)
			continue
		}

		if b == '\n' {
			out.WriteByte('\n')
			w.atLineStart = true
			continue
		}
		out.WriteByte(b)
	}

	if _, err := out.WriteTo(w.w); err != nil {
		w.err = err
	}
	return len(p), nil
}

// Err returns the first underlying write error seen by this writer.
func (w *writer) Err() error {
	return w.err
}

func (w *writer) writeAtLineStart(out *bytes.Buffer, b byte) {
	switch w.prefixKind {
	case prefixNone:
		w.startPrefixOrWrite(out, b)
	case prefixHash:
		w.handleHashPrefix(out, b)
	case prefixBacktick:
		w.handleBacktickPrefix(out, b)
	}
}

func (w *writer) startPrefixOrWrite(out *bytes.Buffer, b byte) {
	switch {
	case b == '\n':
		out.WriteByte('\n')
	case !w.inCodeFence && b == '#':
		w.prefixKind = prefixHash
		w.linePrefix = append(w.linePrefix[:0], b)
	case b == '`':
		w.prefixKind = prefixBacktick
		w.linePrefix = append(w.linePrefix[:0], b)
	default:
		out.WriteByte(b)
		w.atLineStart = false
	}
}

func (w *writer) handleHashPrefix(out *bytes.Buffer, b byte) {
	switch {
	case b == '#' && len(w.linePrefix) < 6:
		w.linePrefix = append(w.linePrefix, b)
		return
	case b == ' ':
		color := w.headingColor(len(w.linePrefix))
		if color != "" {
			out.WriteString(color)
			w.activeHeadingColor = color
		}
		out.Write(w.linePrefix)
		out.WriteByte(' ')
		w.resetPrefix()
		w.atLineStart = false
		return
	}

	w.flushPrefix(out)
	if b == '\n' {
		out.WriteByte('\n')
		w.atLineStart = true
		return
	}
	out.WriteByte(b)
	w.atLineStart = false
}

func (w *writer) handleBacktickPrefix(out *bytes.Buffer, b byte) {
	if b == '`' && len(w.linePrefix) < 3 {
		w.linePrefix = append(w.linePrefix, b)
		if len(w.linePrefix) == 3 {
			out.Write(w.linePrefix)
			w.resetPrefix()
			w.atLineStart = false
			w.inCodeFence = !w.inCodeFence
		}
		return
	}

	w.flushPrefix(out)
	if b == '\n' {
		out.WriteByte('\n')
		w.atLineStart = true
		return
	}
	out.WriteByte(b)
	w.atLineStart = false
}

func (w *writer) flushPrefix(out *bytes.Buffer) {
	if len(w.linePrefix) == 0 {
		w.resetPrefix()
		return
	}
	out.Write(w.linePrefix)
	w.resetPrefix()
}

func (w *writer) resetPrefix() {
	w.linePrefix = w.linePrefix[:0]
	w.prefixKind = prefixNone
}

func (w *writer) headingColor(level int) string {
	switch level {
	case 1:
		return w.scheme.H1Color
	case 2:
		return w.scheme.H2Color
	default:
		return ""
	}
}

func defaultColorScheme() ColorScheme {
	return ColorScheme{
		H1Color: H1Color,
		H2Color: H2Color,
		Reset:   ResetColor,
	}
}
