package terminal_test

import (
	"fmt"
	"log"
	"math"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal/testdata"
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
	screenWriter, err := terminal.NewScreenWriter(math.MaxInt, 0)
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
	// Run each of the basic test cases both with a window height of 0 (no
	// curses) and 1 (with curses).
	var testCases []testdata.TestCase
	for _, windowHeight := range []int{0, 1, 2, 3} {
		for _, tc := range testdata.BasicScreenWritingTestcases {
			testCases = append(testCases, testdata.TestCase{
				Name:       fmt.Sprintf("WindowHeight=%d/%s", windowHeight, tc.Name),
				Write:      tc.Write,
				WantLog:    tc.WantLog,
				ScreenRows: windowHeight,
			})
		}
	}
	// Add some advanced test cases which use cursor positioning or which
	// are specifically testing certain screen dimensions.
	testCases = append(testCases, testdata.AdvancedScreenWritingTestcases...)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			log.SetOutput(&testLogOutput{t: t})
			// Disable timestamps in logs
			log.SetFlags(0)
			log.Printf("Running test case %q", tc.Name)
			screen, err := terminal.NewScreenWriter(math.MaxInt, tc.ScreenRows)
			require.NoError(t, err)
			if tc.ScreenCols > 0 {
				if tc.ScreenRows > 0 {
					screen.Screen.SetSize(tc.ScreenCols, tc.ScreenRows)
				} else {
					screen.Screen.SetSize(tc.ScreenCols, 32)
				}
			}
			for _, s := range tc.Write {
				_, err = screen.Write([]byte(s))
				require.NoError(t, err)
				require.NoError(t, screen.WriteErr)
			}
			require.Equalf(
				t,
				ansiDebugString(tc.WantLog),
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
