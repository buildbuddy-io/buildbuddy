package invocationlog

import (
	"fmt"
	"io"

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

// New creates a new InvocationLog that writes to the given writer.
// The writeListener function is called with each redacted string before
// it is written to the underlying writer. If writeListener is nil, a no-op
// function is used.
func New(writer io.Writer, writeListener func(string)) *InvocationLog {
	if writeListener == nil {
		writeListener = func(s string) {}
	}
	invLog := &InvocationLog{
		writer:        writer,
		writeListener: writeListener,
	}
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
