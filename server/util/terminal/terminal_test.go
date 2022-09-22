package terminal_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"
)

func randomBytes(t *testing.T, n int) []byte {
	s, err := random.RandomString(n)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(s)
}

func TestTruncation(t *testing.T) {
	screenWriter := terminal.NewScreenWriter()

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
