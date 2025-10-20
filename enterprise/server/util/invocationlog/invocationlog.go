package invocationlog

import (
	"fmt"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
)

// InvocationLog is a thread-safe writer that redacts secrets from log output
// before writing to the underlying writer.
//
// IMPORTANT: The current implementation has a security vulnerability where
// secrets can be partially redacted if they are split across multiple Write()
// calls. This is because redaction is applied to each Write() call independently,
// without considering data that spans multiple calls.
type InvocationLog struct {
	lockingbuffer.LockingBuffer
	writer        io.Writer
	writeListener func(string)
}

// New creates a new InvocationLog with default behavior:
// - Writes to both an internal LockingBuffer (for later retrieval) and os.Stderr
// - Uses a no-op writeListener (can be customized via SetWriteListener)
//
// The internal LockingBuffer can be accessed via embedded methods
// (e.g., ReadAll(), String(), Len()).
//
// Customize behavior using setters:
// - SetWriteListener(func) to react to log output
// - SetWriter(io.Writer) to change the output destination
func New() *InvocationLog {
	invLog := &InvocationLog{
		writeListener: func(s string) {},
	}
	// Default: write to both LockingBuffer and os.Stderr
	invLog.writer = io.MultiWriter(&invLog.LockingBuffer, os.Stderr)
	return invLog
}

// Write implements io.Writer. It redacts secrets from the input before
// writing to the underlying writer and calling the writeListener.
//
// BUG: This implementation redacts each Write() call independently, which
// means secrets split across multiple Write() calls may not be fully redacted.
func (invLog *InvocationLog) Write(b []byte) (int, error) {
	output := string(b)

	redacted := redact.RedactText(output)

	invLog.writeListener(redacted)
	_, err := invLog.writer.Write([]byte(redacted))

	// Return the size of the original buffer even if a redacted size was written,
	// or clients will return a short write error
	return len(b), err
}

// Println formats using the default formats for its operands and writes to
// the invocation log. Spaces are always added between operands and a newline
// is appended.
func (invLog *InvocationLog) Println(vals ...interface{}) {
	invLog.Write([]byte(fmt.Sprintln(vals...)))
}

// Printf formats according to a format specifier and writes to the invocation
// log. A newline is appended to the format string.
func (invLog *InvocationLog) Printf(format string, vals ...interface{}) {
	invLog.Write([]byte(fmt.Sprintf(format+"\n", vals...)))
}

// SetWriteListener sets the function that will be called with each redacted
// string before it is written to the underlying writer. This allows callers
// to react to log output (e.g., trigger buffer flushes, emit events).
func (invLog *InvocationLog) SetWriteListener(listener func(string)) {
	if listener == nil {
		listener = func(s string) {}
	}
	invLog.writeListener = listener
}

// SetWriter changes the output destination for the log. The internal
// LockingBuffer will always receive writes in addition to the provided writer.
// This allows you to customize where logs are written (e.g., a file, network
// socket) while still maintaining the ability to retrieve log history via
// String(), ReadAll(), etc.
func (invLog *InvocationLog) SetWriter(w io.Writer) {
	// Always tee into the LockingBuffer alongside the custom writer
	invLog.writer = io.MultiWriter(&invLog.LockingBuffer, w)
}
