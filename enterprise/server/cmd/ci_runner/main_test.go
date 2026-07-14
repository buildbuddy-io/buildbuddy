package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseGitFetchedBytes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		trace2Log string
		want      int64
	}{
		{
			name:      "empty log",
			trace2Log: "",
			want:      0,
		},
		{
			// A fetch with no new objects starts no progress meters, so the
			// log contains only process lifecycle and transfer events.
			name: "fetch with no new objects",
			trace2Log: `{"event":"version","sid":"sid1","thread":"main","time":"2026-07-14T15:23:39.915Z","file":"common-main.c","line":50,"evt":"3","exe":"2.43.0"}
{"event":"start","sid":"sid1","thread":"main","time":"2026-07-14T15:23:39.915Z","file":"common-main.c","line":51,"t_abs":0.001,"argv":["git","fetch","--force","origin","master"]}
{"event":"data","sid":"sid1","thread":"main","time":"2026-07-14T15:23:39.917Z","file":"connect.c","line":175,"t_abs":0.002,"t_rel":0.001,"nesting":2,"category":"transfer","key":"negotiated-version","value":"2"}
{"event":"exit","sid":"sid1","thread":"main","time":"2026-07-14T15:23:40.100Z","file":"git.c","line":712,"t_abs":0.185,"code":0}
`,
			want: 0,
		},
		{
			// Receiving a pack logs a "total_bytes" data event under the
			// "Receiving objects" progress region when the meter completes.
			// Sibling data events such as "total_objects" are not byte
			// counts and are expected to be ignored.
			name: "received pack",
			trace2Log: `{"event":"region_enter","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:23:59.947Z","file":"progress.c","line":270,"repo":1,"nesting":1,"category":"progress","label":"Receiving objects"}
{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":341,"repo":1,"t_abs":0.153,"t_rel":0.153,"nesting":2,"category":"progress","key":"total_objects","value":"155"}
{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"t_abs":0.153,"t_rel":0.153,"nesting":2,"category":"progress","key":"total_bytes","value":"12985331"}
{"event":"region_leave","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":348,"repo":1,"t_rel":0.153,"nesting":1,"category":"progress","label":"Receiving objects"}
`,
			want: 12985331,
		},
		{
			// Small fetches unpack objects directly rather than indexing a
			// pack; the "Unpacking objects" progress meter also logs a
			// "total_bytes" data event.
			name: "unpacked objects",
			trace2Log: `{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"t_abs":0.010,"t_rel":0.010,"nesting":2,"category":"progress","key":"total_bytes","value":"1034"}
`,
			want: 1034,
		},
		{
			// A fetch may receive multiple packs (e.g. submodule fetches
			// append events from their own processes to the same log).
			// Expect the byte counts to be summed.
			name: "multiple packs",
			trace2Log: `{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"nesting":2,"category":"progress","key":"total_bytes","value":"1000"}
{"event":"data","sid":"sid1/sid3","thread":"main","time":"2026-07-14T15:24:01.100Z","file":"progress.c","line":345,"repo":1,"nesting":2,"category":"progress","key":"total_bytes","value":"234"}
`,
			want: 1234,
		},
		{
			// Progress meters without throughput (such as "Resolving
			// deltas") log only object counts. A "total_bytes" key under a
			// different category should not be counted as fetched bytes.
			name: "non-progress categories ignored",
			trace2Log: `{"event":"data","sid":"sid1","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":341,"repo":1,"nesting":2,"category":"progress","key":"total_objects","value":"155"}
{"event":"data","sid":"sid1","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"index-pack.c","line":100,"repo":1,"nesting":2,"category":"pack","key":"total_bytes","value":"999"}
`,
			want: 0,
		},
		{
			// Concurrent processes appending to the log can in rare cases
			// interleave partial lines. Expect malformed lines to be skipped
			// without affecting other events.
			name: "malformed lines skipped",
			trace2Log: `{"event":"data","sid":"sid1","thread":"main","cat
{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"nesting":2,"category":"progress","key":"total_bytes","value":"4096"}
not json at all
{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"nesting":2,"category":"progress","key":"total_bytes","value":"not-a-number"}
`,
			want: 4096,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseGitFetchedBytes(strings.NewReader(tc.trace2Log)))
		})
	}
}
