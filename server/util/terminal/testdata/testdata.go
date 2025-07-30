package testdata

const (
	CUU  = "\x1b[A"  // Cursor Up
	CHA  = "\x1b[G"  // Cursor Horizontal Absolute (go to column 1)
	EL_2 = "\x1b[2K" // Erase in line; 2="Entire line"
	ED_2 = "\x1b[2J" // Erase in display; 2="Entire display"
	CUP  = "\x1b[H"  // Cursor Position (go to row 1, column 1)
)

type TestCase struct {
	Name       string
	ScreenRows int
	ScreenCols int
	Write      []string
	WantLog    string
}

var (
	BasicScreenWritingTestcases = []TestCase{
		{
			Name:    "single write with space",
			Write:   []string{" "},
			WantLog: " ",
		},
		{
			Name:    "single blankline write",
			Write:   []string{"\n"},
			WantLog: "\n",
		},
		{
			Name:    "multiple blankline writes",
			Write:   []string{"\n", "\n"},
			WantLog: "\n\n",
		},
		{
			Name:    "multiple writes with multiple lines",
			Write:   []string{"1\n", "2\n"},
			WantLog: "1\n2\n",
		},
		{
			Name:    "single write with multiple lines",
			Write:   []string{"1\n2\n"},
			WantLog: "1\n2\n",
		},
		{
			Name:    "multiple writes with multiple lines and double-newline",
			Write:   []string{"1\n\n", "2\n"},
			WantLog: "1\n\n2\n",
		},
		{
			Name:    "single writes with multiple lines and double-newline",
			Write:   []string{"1\n\n2\n"},
			WantLog: "1\n\n2\n",
		},
		{
			Name:    "multiple writes with mix of trailing and leading whitespace",
			Write:   []string{"1", "2\n", "3\n ", "4\n\n", "5 \n", " 6"},
			WantLog: "12\n3\n 4\n\n5\n 6",
		},
	}
	AdvancedScreenWritingTestcases = []TestCase{
		{
			Name:       "scrollout of single line wrapped to multiple rows",
			ScreenCols: 1,
			ScreenRows: 2,
			// The first line "ab" should be split and wrapped to the second row
			// since maxCols is 1. We should get "ab" back as a single line;
			// it should not be artificially wrapped.
			Write:   []string{"ab\n", "c\n", "d"},
			WantLog: "ab\nc\nd",
		},
		{
			Name:       "overwrite line contents",
			Write:      []string{"123456789" + EL_2 + CHA + "ABC"},
			WantLog:    "ABC",
			ScreenRows: 1,
		},
		{
			Name:       "overwrite line contents with newline",
			Write:      []string{"123456789" + EL_2 + CHA + "ABC\n"},
			WantLog:    "ABC\n",
			ScreenRows: 1,
		},
		{
			Name: "overwrite multiple line contents",
			Write: []string{
				// Write two lines
				"123456789\n",
				"ABCDEFG",
				// Go back up to the first line
				CUU + CUU,
				// Clear the current line and write "Hello" on its own line
				EL_2 + CHA + "Hello\n",
				// Clear the current line and write "World" on its own line
				EL_2 + CHA + "World\n"},
			WantLog:    "Hello\nWorld\n",
			ScreenRows: 2,
		},
		{
			Name: "overwrite multiple line contents via screen clearing",
			Write: []string{
				// Write two lines
				"1234",
				"56789\n",
				"ABCDEFG",
				// Clear screen and position cursor at top-left
				ED_2 + CUP,
				// Write "Hello" on its own line
				"Hello\n",
				// Write "World" on its own line
				"World\n"},
			WantLog:    "Hello\nWorld\n",
			ScreenRows: 2,
		},
		{
			Name:       "reset sequence at end of line",
			Write:      []string{"\x1b[32mINFO: ...\x1b[0m\n"},
			WantLog:    "\x1b[32mINFO: ...\n",
			ScreenRows: 1,
		},
	}
)


