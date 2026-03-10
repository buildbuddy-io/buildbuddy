package markdown

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// testColorScheme uses readable marker strings (not ANSI escapes) so expected
// output in tests is easy to understand.
var testColorScheme = ColorScheme{
	H1Color: "[H1]",
	H2Color: "[H2]",
	Reset:   "[RESET]",
}

var testOptions = &Options{
	ColorScheme: &testColorScheme,
}

func TestWriter_ColorizesHeadings(t *testing.T) {
	var buf bytes.Buffer
	w := Writer(&buf, testOptions)
	if err := writeLines(w, "# Title", "## Subtitle", "### Tertiary"); err != nil {
		t.Fatalf("write markdown: %v", err)
	}

	want := joinLines(
		"[H1]# Title[RESET]",
		"[H2]## Subtitle[RESET]",
		"### Tertiary",
	)
	if got := buf.String(); got != want {
		t.Fatalf("unexpected output:\nwant: %q\ngot:  %q", want, got)
	}
}

func TestWriter_DoesNotColorizeInsideCodeFence(t *testing.T) {
	var buf bytes.Buffer
	w := Writer(&buf, testOptions)
	if err := writeLines(w, "```bash", "# inside", "## still inside", "```", "# outside"); err != nil {
		t.Fatalf("write markdown: %v", err)
	}

	want := joinLines(
		"```bash",
		"# inside",
		"## still inside",
		"```",
		"[H1]# outside[RESET]",
	)
	if got := buf.String(); got != want {
		t.Fatalf("unexpected output:\nwant: %q\ngot:  %q", want, got)
	}
}

func TestWriter_StreamingAcrossChunkBoundaries(t *testing.T) {
	var buf bytes.Buffer
	w := Writer(&buf, testOptions)

	chunks := []string{
		"`",
		"``bash\n# in",
		"side\n```\n# out",
		"side\n## sub\n",
	}
	for _, chunk := range chunks {
		if _, err := io.WriteString(w, chunk); err != nil {
			t.Fatalf("write chunk %q: %v", chunk, err)
		}
	}

	want := joinLines(
		"```bash",
		"# inside",
		"```",
		"[H1]# outside[RESET]",
		"[H2]## sub[RESET]",
	)
	if got := buf.String(); got != want {
		t.Fatalf("unexpected output:\nwant: %q\ngot:  %q", want, got)
	}
}

func TestWriter_StoresFirstErrorAndSkipsSubsequentWrites(t *testing.T) {
	underlyingErr := errors.New("boom")
	dst := &failingWriter{err: underlyingErr}
	w := Writer(dst, nil)

	if _, err := io.WriteString(w, "# first\n"); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if _, err := io.WriteString(w, "# second\n"); err != nil {
		t.Fatalf("write second: %v", err)
	}

	if got, want := dst.writeCalls, 1; got != want {
		t.Fatalf("unexpected write calls: got %d, want %d", got, want)
	}
	if err := w.Err(); err != underlyingErr {
		t.Fatalf("unexpected stored error: got %v, want %v", err, underlyingErr)
	}
}

func writeLines(w io.Writer, lines ...string) error {
	for _, line := range lines {
		if _, err := io.WriteString(w, line+"\n"); err != nil {
			return err
		}
	}
	return nil
}

func joinLines(lines ...string) string {
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}

type failingWriter struct {
	writeCalls int
	err        error
}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.writeCalls++
	return 0, w.err
}
