package terminal

import (
	"unicode"
	"unicode/utf8"
)

const (
	MODE_NORMAL  = iota
	MODE_ESCAPE  = iota
	MODE_CONTROL = iota
	MODE_OSC     = iota
	MODE_CHARSET = iota
	MODE_APC     = iota
)

// Stateful ANSI parser
type parser struct {
	mode                 int
	screen               *screen
	ansi                 []byte
	cursor               int
	escapeStartedAt      int
	instructions         []string
	instructionStartedAt int
}

/*
 * How this state machine works:
 *
 * We start in MODE_NORMAL. We're not inside an escape sequence. In this mode
 * most input is written directly to the screen. If we receive a newline,
 * backspace or other cursor-moving signal, we let the screen know so that it
 * can change the location of its cursor accordingly.
 *
 * If we're in MODE_NORMAL and we receive an escape character (\x1b) we enter
 * MODE_ESCAPE. The following character could start an escape sequence, a
 * control sequence, an operating system command, or be invalid or not understood.
 *
 * If we're in MODE_ESCAPE we look for three possible characters:
 *
 * 1. For `[` we enter MODE_CONTROL and start looking for a control sequence.
 * 2. For `]` we enter MODE_OSC and look for an operating system command.
 * 3. For `(` or ')' we enter MODE_CHARSET and look for a character set name.
 * 4. For `_` we enter MODE_APC and parse the rest of the custom control sequence
 *
 * In all cases we start our instruction buffer. The instruction buffer is used
 * to store the individual characters that make up ANSI instructions before
 * sending them to the screen. If we receive neither of these characters, we
 * treat this as an invalid or unknown escape and return to MODE_NORMAL.
 *
 * If we're in MODE_CONTROL, we expect to receive a sequence of parameters and
 * then a terminal alphabetic character looking like 1;30;42m. That's an
 * instruction to turn on bold, set the foreground colour to black and the
 * background colour to green. We receive these characters one by one turning
 * the parameters into instruction parts (1, 30, 42) followed by an instruction
 * type (m). Once the instruction type is received we send it and its parts to
 * the screen and return to MODE_NORMAL.
 *
 * If we're in MODE_OSC, we expect to receive a sequence of characters up to
 * and including a bell (\a). We skip forward until this bell is reached, then
 * send everything from when we entered MODE_OSC up to the bell to
 * parseElementSequence and return to MODE_NORMAL.
 *
 * If we're in MODE_CHARSET we simply discard the next character which would
 * normally designate the character set.
 */

func parseANSIToScreen(s *screen, ansi []byte) {
	p := parser{mode: MODE_NORMAL, ansi: ansi, screen: s}
	p.mode = MODE_NORMAL
	length := len(p.ansi)
	for p.cursor = 0; p.cursor < length; {
		char, charLen := utf8.DecodeRune(p.ansi[p.cursor:])

		switch p.mode {
		case MODE_ESCAPE:
			// We've received an escape character but aren't inside an escape sequence yet
			p.handleEscape(char)
		case MODE_CONTROL:
			// We're inside a control sequence - figure out its code and its instructions.
			p.handleControlSequence(char)
		case MODE_OSC:
			// We're inside an operating system command, capture until we hit a bell character
			p.handleOperatingSystemCommand(char)
		case MODE_CHARSET:
			// We're inside a charset sequence, capture the next character.
			p.handleCharset(char)
		case MODE_APC:
			// We're inside a custom escape sequence
			p.handleApplicationProgramCommand(char)
		case MODE_NORMAL:
			// Outside of an escape sequence entirely, normal input
			p.handleNormal(char)
		}

		p.cursor += charLen
	}
}

func (p *parser) handleCharset(char rune) {
	p.mode = MODE_NORMAL
}

func (p *parser) handleOperatingSystemCommand(char rune) {
	if char != '\a' {
		return
	}
	p.mode = MODE_NORMAL

	// Bell received, stop parsing our potential image
	image, err := parseElementSequence(string(p.ansi[p.instructionStartedAt:p.cursor]))

	if image == nil && err == nil {
		// No image & no error, nothing to render
		return
	}

	ownLine := image == nil || image.elementType != ELEMENT_LINK

	if ownLine {
		// Images (or the error encountered) should appear on their own line
		if p.screen.x != 0 {
			p.screen.newLine()
		}
		p.screen.clear(p.screen.y, screenStartOfLine, screenEndOfLine)
	}

	if err != nil {
		p.screen.appendMany([]rune("*** Error parsing custom element escape sequence: "))
		p.screen.appendMany([]rune(err.Error()))
	} else {
		p.screen.appendElement(image)
	}

	if ownLine {
		p.screen.newLine()
	}
}

func (p *parser) handleApplicationProgramCommand(char rune) {
	if char != '\a' && char != '\x07' {
		return
	}
	p.mode = MODE_NORMAL

	// Bell received, stop parsing our potential image
	el, err := parseBuildkiteElementSequence(string(p.ansi[p.instructionStartedAt:p.cursor]))

	if el == nil && err == nil {
		// No element & no error, nothing to render
		return
	}

	if err != nil {
		p.screen.appendMany([]rune("*** Error parsing buildkite element escape sequence: "))
		p.screen.appendMany([]rune(err.Error()))
	} else {
		p.screen.appendElement(el)
	}
}

func (p *parser) handleControlSequence(char rune) {
	char = unicode.ToUpper(char)
	switch char {
	case '?', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// Part of an instruction
	case ';':
		p.addInstruction()
		p.instructionStartedAt = p.cursor + utf8.RuneLen(';')
	case 'Q', 'K', 'G', 'A', 'B', 'C', 'D', 'M':
		p.addInstruction()
		p.screen.applyEscape(char, p.instructions)
		p.mode = MODE_NORMAL
	case 'H', 'L':
		// Set/reset mode (SM/RM), ignore and continue
		p.mode = MODE_NORMAL
	default:
		// unrecognized character, abort the escapeCode
		p.cursor = p.escapeStartedAt
		p.mode = MODE_NORMAL
	}
}

func (p *parser) handleNormal(char rune) {
	switch char {
	case '\n':
		p.screen.newLine()
	case '\r':
		p.screen.carriageReturn()
	case '\b':
		p.screen.backspace()
	case '\x1b':
		p.escapeStartedAt = p.cursor
		p.mode = MODE_ESCAPE
	default:
		p.screen.append(char)
	}
}

func (p *parser) handleEscape(char rune) {
	switch char {
	case '[':
		p.instructionStartedAt = p.cursor + utf8.RuneLen('[')
		p.instructions = make([]string, 0, 1)
		p.mode = MODE_CONTROL
	case ']':
		p.instructionStartedAt = p.cursor + utf8.RuneLen('[')
		p.mode = MODE_OSC
	case ')', '(':
		p.instructionStartedAt = p.cursor + utf8.RuneLen('(')
		p.mode = MODE_CHARSET
	case '_':
		p.instructionStartedAt = p.cursor + utf8.RuneLen('[')
		p.mode = MODE_APC
	default:
		// Not an escape code, false alarm
		p.cursor = p.escapeStartedAt
		p.mode = MODE_NORMAL
	}
}

func (p *parser) addInstruction() {
	instruction := string(p.ansi[p.instructionStartedAt:p.cursor])
	if instruction != "" {
		p.instructions = append(p.instructions, instruction)
	}
}
