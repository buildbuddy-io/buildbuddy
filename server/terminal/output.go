package terminal

import (
	"bytes"
	"strings"
)

type outputBuffer struct {
	buf bytes.Buffer
}

func (b *outputBuffer) appendANSIStyle(n node) {
	for _, code := range n.style.asANSICodes() {
		b.buf.Write([]byte(code))
	}
}

func (b *outputBuffer) resetANSI() {
	b.buf.Write([]byte("\u001b[0m"))
}

// Append a character to our outputbuffer, escaping HTML bits as necessary.
func (b *outputBuffer) appendChar(char rune) {
	switch char {
	case '&':
		b.buf.WriteString("&amp;")
	case '\'':
		b.buf.WriteString("&#39;")
	case '<':
		b.buf.WriteString("&lt;")
	case '>':
		b.buf.WriteString("&gt;")
	case '"':
		b.buf.WriteString("&quot;")
	case '/':
		b.buf.WriteString("&#47;")
	default:
		b.buf.WriteRune(char)
	}
}

func outputLineAsANSI(line []node) string {
	var styleApplied bool
	var lineBuf outputBuffer

	for idx, node := range line {
		if idx == 0 && !node.style.isEmpty() {
			lineBuf.appendANSIStyle(node)
			styleApplied = true
		} else if idx > 0 {
			previous := line[idx-1]
			if !node.hasSameStyle(previous) {
				if styleApplied {
					lineBuf.resetANSI()
					styleApplied = false
				}
				if !node.style.isEmpty() {
					lineBuf.appendANSIStyle(node)
					styleApplied = true
				}
			}
		}
		lineBuf.buf.WriteRune(node.blob)
	}
	if styleApplied {
		lineBuf.resetANSI()
	}
	return strings.TrimRight(lineBuf.buf.String(), " \t")
}
