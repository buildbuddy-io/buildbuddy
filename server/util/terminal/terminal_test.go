package terminal_test

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"
	"github.com/stretchr/testify/require"
)

func randomBytes(t *testing.T, n int) []byte {
	s, err := random.RandomString(n)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(s)
}

func TestTruncation(t *testing.T) {
	screenWriter, err := terminal.NewScreenWriter(0)
	require.NoError(t, err)

	screenWriter.Write(randomBytes(t, 80))
	screenWriter.Write(randomBytes(t, 10))

	buf0 := randomBytes(t, 90)
	buf0 = append(buf0, []byte{'\x1b', '_'}...)
	screenWriter.Write(buf0)

	buf1 := randomBytes(t, 10)
	buf1 = append(buf1, []byte{'\a'}...)
	screenWriter.Write(buf1)

	// verify that we got here, no panic.
}

func TestComplexScreenWriting(t *testing.T) {
	type testCase struct {
		name       string
		screenRows int
		screenCols int
		write      []string
		wantLog    string
	}
	basicTestCases := []testCase{
		{
			name:    "single write with space",
			write:   []string{" "},
			wantLog: " ",
		},
		{
			name:    "single blankline write",
			write:   []string{"\n"},
			wantLog: "\n",
		},
		{
			name:    "multiple blankline writes",
			write:   []string{"\n", "\n"},
			wantLog: "\n\n",
		},
		{
			name:    "multiple writes with multiple lines",
			write:   []string{"1\n", "2\n"},
			wantLog: "1\n2\n",
		},
		{
			name:    "single write with multiple lines",
			write:   []string{"1\n2\n"},
			wantLog: "1\n2\n",
		},
		{
			name:    "multiple writes with multiple lines and double-newline",
			write:   []string{"1\n\n", "2\n"},
			wantLog: "1\n\n2\n",
		},
		{
			name:    "single writes with multiple lines and double-newline",
			write:   []string{"1\n\n2\n"},
			wantLog: "1\n\n2\n",
		},
		{
			name:    "multiple writes with mix of trailing and leading whitespace",
			write:   []string{"1", "2\n", "3\n ", "4\n\n", "5 \n", " 6"},
			wantLog: "12\n3\n 4\n\n5\n 6",
		},
	}
	// Run each of the basic test cases both with a window height of 0 (no
	// curses) and 1 (with curses).
	var testCases []testCase
	for _, windowHeight := range []int{0, 1, 2, 3} {
		for _, tc := range basicTestCases {
			testCases = append(testCases, testCase{
				name:       fmt.Sprintf("WindowHeight=%d/%s", windowHeight, tc.name),
				write:      tc.write,
				wantLog:    tc.wantLog,
				screenRows: windowHeight,
			})
		}
	}
	// Add some advanced test cases which use cursor positioning or which
	// are specifically testing certain screen dimensions.
	const (
		CUU  = "\x1b[A"  // Cursor Up
		CHA  = "\x1b[G"  // Cursor Horizontal Absolute (go to column 1)
		EL_2 = "\x1b[2K" // Erase in line; 2="Entire line"
		ED_2 = "\x1b[2J" // Erase in display; 2="Entire display"
		CUP  = "\x1b[H"  // Cursor Position (go to row 1, column 1)
	)
	testCases = append(testCases, []testCase{
		{
			name:       "scrollout of single line wrapped to multiple rows",
			screenCols: 1,
			screenRows: 2,
			// The first line "ab" should be split and wrapped to the second row
			// since maxCols is 1. We should get "ab" back as a single line;
			// it should not be artificially wrapped.
			write:   []string{"ab\n", "c\n", "d"},
			wantLog: "ab\nc\nd",
		},
		{
			name:       "overwrite line contents",
			write:      []string{"123456789" + EL_2 + CHA + "ABC"},
			wantLog:    "ABC",
			screenRows: 1,
		},
		{
			name:       "overwrite line contents with newline",
			write:      []string{"123456789" + EL_2 + CHA + "ABC\n"},
			wantLog:    "ABC\n",
			screenRows: 1,
		},
		{
			name: "overwrite multiple line contents",
			write: []string{
				// Write two lines
				"123456789\n",
				"ABCDEFG",
				// Go back up to the first line
				CUU + CUU,
				// Clear the current line and write "Hello" on its own line
				EL_2 + CHA + "Hello\n",
				// Clear the current line and write "World" on its own line
				EL_2 + CHA + "World\n"},
			wantLog:    "Hello\nWorld\n",
			screenRows: 2,
		},
		{
			name: "overwrite multiple line contents via screen clearing",
			write: []string{
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
			wantLog:    "Hello\nWorld\n",
			screenRows: 2,
		},
		{
			name:       "reset sequence at end of line",
			write:      []string{"\x1b[32mINFO: ...\x1b[0m\n"},
			wantLog:    "\x1b[32mINFO: ...\n",
			screenRows: 1,
		},
	}...)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			log.SetOutput(&testLogOutput{t: t})
			// Disable timestamps in logs
			log.SetFlags(0)
			log.Printf("Running test case %q", tc.name)
			screen, err := terminal.NewScreenWriter(tc.screenRows)
			require.NoError(t, err)
			if tc.screenCols > 0 {
				if tc.screenRows > 0 {
					screen.Screen.SetSize(tc.screenCols, tc.screenRows)
				} else {
					screen.Screen.SetSize(tc.screenCols, terminal.Lines)
				}
			}
			for _, s := range tc.write {
				_, err = screen.Write([]byte(s))
				require.NoError(t, err)
				require.NoError(t, screen.WriteErr)
			}
			require.Equalf(
				t,
				ansiDebugString(tc.wantLog),
				ansiDebugString(screen.OutputAccumulator.String()+screen.Render()),
				"scrollout was:\n--------\n%s\n--------\nrender was:\n--------\n%s\n--------\n",
				screen.OutputAccumulator.String(),
				screen.Render(),
			)
		})
	}
}

// ansiDebugString replaces ANSI escape sequences with a string representation
// that can be displayed as a diff without messing up the terminal.
func ansiDebugString(s string) string {
	// \x1b[0m is interpreted the same as \x1b[m. Normalize these to make
	// assertions easier.
	s = strings.ReplaceAll(s, "\x1b[0m", "\x1b[m")
	// Quote the string, so escape sequences like \x1b[32m show up as text
	// instead of actual colors.
	content := fmt.Sprintf("%q", s)
	// Remove the surrounding quotes.
	content = content[1 : len(content)-1]
	// Preserve newlines so that it's easier to diff the expected vs. actual
	// output.
	content = strings.ReplaceAll(content, "\\n", "\n")
	return content
}

type testLogOutput struct{ t *testing.T }

func (w *testLogOutput) Write(p []byte) (int, error) {
	w.t.Log(string(p[:len(p)-1]))
	return len(p), nil
}
