package terminal

import "strconv"

var emptyStyle = style{colors: []string{}, meta: styleMeta{}}

type styleMeta struct {
	fgColor   uint8
	bgColor   uint8
	fgColorX  bool
	bgColorX  bool
	bold      bool
	faint     bool
	italic    bool
	underline bool
	strike    bool
	blink     bool
}

type style struct {
	colors []string
	meta   styleMeta
}

const (
	COLOR_NORMAL        = iota
	COLOR_GOT_38_NEED_5 = iota
	COLOR_GOT_48_NEED_5 = iota
	COLOR_GOT_38        = iota
	COLOR_GOT_48        = iota
)

// True if both styles are equal
func (s *style) isEqual(o *style) bool {
	return s.meta == o.meta
}

// ANSI codes that make up the style
func (s *style) asANSICodes() []string {
	var codes []string

	for _, color := range s.colors {
		codes = append(codes, "\u001b["+color+"m")
	}
	return codes
}

// CSS classes that make up the style
func (s *style) asClasses() []string {
	var styles []string

	if s.meta.fgColor > 0 && s.meta.fgColor < 38 && !s.meta.fgColorX {
		styles = append(styles, "term-fg"+strconv.Itoa(int(s.meta.fgColor)))
	}
	if s.meta.fgColor > 38 && !s.meta.fgColorX {
		styles = append(styles, "term-fgi"+strconv.Itoa(int(s.meta.fgColor)))

	}
	if s.meta.fgColorX {
		styles = append(styles, "term-fgx"+strconv.Itoa(int(s.meta.fgColor)))

	}

	if s.meta.bgColor > 0 && s.meta.bgColor < 48 && !s.meta.bgColorX {
		styles = append(styles, "term-bg"+strconv.Itoa(int(s.meta.bgColor)))
	}
	if s.meta.bgColor > 48 && !s.meta.bgColorX {
		styles = append(styles, "term-bgi"+strconv.Itoa(int(s.meta.bgColor)))
	}
	if s.meta.bgColorX {
		styles = append(styles, "term-bgx"+strconv.Itoa(int(s.meta.bgColor)))
	}

	if s.meta.bold {
		styles = append(styles, "term-fg1")
	}
	if s.meta.faint {
		styles = append(styles, "term-fg2")
	}
	if s.meta.italic {
		styles = append(styles, "term-fg3")
	}
	if s.meta.underline {
		styles = append(styles, "term-fg4")
	}
	if s.meta.blink {
		styles = append(styles, "term-fg5")
	}
	if s.meta.strike {
		styles = append(styles, "term-fg9")
	}

	return styles
}

// True if style is empty
func (s *style) isEmpty() bool {
	return s.meta == styleMeta{}
}

// Add colours to an existing style, potentially returning
// a new style.
func (s *style) color(colors []string) *style {
	if len(colors) == 1 && (colors[0] == "0" || colors[0] == "") {
		// Shortcut for full style reset
		return &emptyStyle
	}

	newStyle := style(*s)
	s = &newStyle
	s.colors = append(s.colors, colors...)
	color_mode := COLOR_NORMAL

	for _, ccs := range colors {
		// If multiple colors are defined, i.e. \e[30;42m\e then loop through each
		// one, and assign it to s.fgColor or s.bgColor
		cc, err := strconv.ParseUint(ccs, 10, 8)
		if err != nil {
			continue
		}

		// State machine for XTerm colors, eg 38;5;150
		switch color_mode {
		case COLOR_GOT_38_NEED_5:
			if cc == 5 {
				color_mode = COLOR_GOT_38
			} else {
				color_mode = COLOR_NORMAL
			}
			continue
		case COLOR_GOT_48_NEED_5:
			if cc == 5 {
				color_mode = COLOR_GOT_48
			} else {
				color_mode = COLOR_NORMAL
			}
			continue
		case COLOR_GOT_38:
			s.meta.fgColor = uint8(cc)
			s.meta.fgColorX = true
			color_mode = COLOR_NORMAL
			continue
		case COLOR_GOT_48:
			s.meta.bgColor = uint8(cc)
			s.meta.bgColorX = true
			color_mode = COLOR_NORMAL
			continue
		}

		switch cc {
		case 0:
			// Reset all styles - don't use &emptyStyle here as we could end up adding colours
			// in this same action.
			s = &style{}
		case 1:
			s.meta.bold = true
			s.meta.faint = false
		case 2:
			s.meta.faint = true
			s.meta.bold = false
		case 3:
			s.meta.italic = true
		case 4:
			s.meta.underline = true
		case 5, 6:
			s.meta.blink = true
		case 9:
			s.meta.strike = true
		case 21, 22:
			s.meta.bold = false
			s.meta.faint = false
		case 23:
			s.meta.italic = false
		case 24:
			s.meta.underline = false
		case 25:
			s.meta.blink = false
		case 29:
			s.meta.strike = false
		case 38:
			color_mode = COLOR_GOT_38_NEED_5
		case 39:
			s.meta.fgColor = 0
			s.meta.fgColorX = false
		case 48:
			color_mode = COLOR_GOT_48_NEED_5
		case 49:
			s.meta.bgColor = 0
			s.meta.bgColorX = false
		case 30, 31, 32, 33, 34, 35, 36, 37, 90, 91, 92, 93, 94, 95, 96, 97:
			s.meta.fgColor = uint8(cc)
			s.meta.fgColorX = false
		case 40, 41, 42, 43, 44, 45, 46, 47, 100, 101, 102, 103, 104, 105, 106, 107:
			s.meta.bgColor = uint8(cc)
			s.meta.bgColorX = false
		}
	}
	return s
}
