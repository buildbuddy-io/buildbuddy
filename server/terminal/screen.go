package terminal

import (
	"math"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// A terminal 'screen'. Current cursor position, cursor style, and characters
type screen struct {
	style    *style
	screen   [][]node
	x        int
	y        int
	maxLines int
}

const screenEndOfLine = -1
const screenStartOfLine = 0

func newScreen(maxLines int) *screen {
	return &screen{
		maxLines: maxLines,
	}
}

// Clear part (or all) of a line on the screen
func (s *screen) clear(y int, xStart int, xEnd int) {
	if len(s.screen) <= y {
		return
	}

	if xStart == screenStartOfLine && xEnd == screenEndOfLine {
		s.screen[y] = make([]node, 0, 80)
	} else {
		line := s.screen[y]

		if xEnd == screenEndOfLine {
			xEnd = len(line) - 1
		}
		for i := xStart; i <= xEnd && i < len(line); i++ {
			line[i] = emptyNode
		}
	}
}

// "Safe" parseint for parsing ANSI instructions
func pi(s string) int {
	if s == "" {
		return 1
	}
	i, _ := strconv.ParseInt(s, 10, 8)
	return int(i)
}

// Move the cursor up, if we can
func (s *screen) up(i string) {
	s.y -= pi(i)
	if s.y < 0 {
		log.Warningf("Screen attempted to move cursor above the top of the screen by %d line(s).", -s.y)
		s.y = 0
	}
}

// Move the cursor down
func (s *screen) down(i string) {
	s.y += pi(i)
}

// Move the cursor forward on the line
func (s *screen) forward(i string) {
	s.x += pi(i)
}

// Move the cursor backward, if we can
func (s *screen) backward(i string) {
	s.x -= pi(i)
	s.x = int(math.Max(0, float64(s.x)))
}

// Add rows to our screen if necessary
func (s *screen) growScreenHeight() {
	for i := len(s.screen); i <= s.y; i++ {
		s.screen = append(s.screen, make([]node, 0, 80))
	}
	if s.maxLines > 0 {
		extraLines := len(s.screen) - s.maxLines
		if extraLines > 0 {
			s.screen = s.screen[extraLines:]
			s.y -= extraLines
			if s.y < 0 {
				log.Warningf("Screen did not retain enough lines to maintain the cursor position. Screen retained %d too few line(s).", -s.y)
				s.y = 0
			}
		}
	}
}

// Add columns to our current line if necessary
func (s *screen) growLineWidth(line []node) []node {
	for i := len(line); i <= s.x; i++ {
		line = append(line, emptyNode)
	}
	return line
}

// Write a character to the screen's current X&Y, along with the current screen style
func (s *screen) write(data rune) {
	s.growScreenHeight()

	line := s.screen[s.y]
	line = s.growLineWidth(line)

	line[s.x] = node{blob: data, style: s.style}
	s.screen[s.y] = line
}

// Append a character to the screen
func (s *screen) append(data rune) {
	s.write(data)
	s.x++
}

// Append multiple characters to the screen
func (s *screen) appendMany(data []rune) {
	for _, char := range data {
		s.append(char)
	}
}

func (s *screen) appendElement(i *element) {
	s.growScreenHeight()
	line := s.growLineWidth(s.screen[s.y])

	line[s.x] = node{style: s.style, elem: i}
	s.screen[s.y] = line
	s.x++
}

// Apply color instruction codes to the screen's current style
func (s *screen) color(i []string) {
	s.style = s.style.color(i)
}

// Apply an escape sequence to the screen
func (s *screen) applyEscape(code rune, instructions []string) {
	if len(instructions) == 0 {
		// Ensure we always have a first instruction
		instructions = []string{""}
	}

	switch code {
	case 'M':
		s.color(instructions)
	case 'G':
		s.x = 0
	case 'K':
		switch instructions[0] {
		case "0", "":
			s.clear(s.y, s.x, screenEndOfLine)
		case "1":
			s.clear(s.y, screenStartOfLine, s.x)
		case "2":
			s.clear(s.y, screenStartOfLine, screenEndOfLine)
		}
	case 'A':
		s.up(instructions[0])
	case 'B':
		s.down(instructions[0])
	case 'C':
		s.forward(instructions[0])
	case 'D':
		s.backward(instructions[0])
	}
}

// Parse ANSI input, populate our screen buffer with nodes
func (s *screen) parse(ansi []byte) {
	s.style = &emptyStyle

	parseANSIToScreen(s, ansi)
}

func (s *screen) asANSI() []byte {
	var lines []string

	for _, line := range s.screen {
		lines = append(lines, outputLineAsANSI(line))
	}

	return []byte(strings.Join(lines, "\n"))
}

func (s *screen) newLine() {
	s.x = 0
	s.y++
}

func (s *screen) carriageReturn() {
	s.x = 0
}

func (s *screen) backspace() {
	if s.x > 0 {
		s.x--
	}
}

func (s *screen) popExtraLines(linesToRetain int) []byte {
	extraLines := len(s.screen) - linesToRetain
	if extraLines > s.y {
		log.Warningf("Screen attempted to pop line containing the current cursor position. Attempted to retain too few lines by %d line(s).", extraLines-s.y)
		extraLines = s.y
	}
	if extraLines < 1 {
		return []byte{}
	}
	poppedLines := (&screen{screen: s.screen[:extraLines]}).asANSI()
	s.screen = s.screen[extraLines:]
	s.y -= extraLines
	return poppedLines
}
