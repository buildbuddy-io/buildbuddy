package persistentworker

import "testing"

func TestStderrDebugString(t *testing.T) {
	tests := []struct {
		name   string
		stderr []byte
		want   string
	}{
		{
			name: "empty",
			want: "<empty>",
		},
		{
			name:   "valid UTF-8",
			stderr: []byte("hello, 世界"),
			want:   "hello, 世界",
		},
		{
			name:   "invalid UTF-8",
			stderr: []byte{'a', 0xff, 'b'},
			want:   "a\uFFFDb",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := &Worker{}
			if _, err := w.stderr.Write(test.stderr); err != nil {
				t.Fatalf("write stderr: %s", err)
			}
			if got := w.stderrDebugString(); got != test.want {
				t.Fatalf("stderrDebugString() = %q, want %q", got, test.want)
			}
		})
	}
}
