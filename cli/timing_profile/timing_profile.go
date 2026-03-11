package timing_profile

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/trace_events"
)

const DefaultTopN = trace_events.DefaultTopN

type NamedDuration = trace_events.NamedDuration
type ThreadDuration = trace_events.ThreadDuration
type Summary = trace_events.Summary

func Summarize(rawProfile []byte, profileName string, topN int) (Summary, bool, error) {
	reader, gzipCompressed, err := maybeDecompressGzip(rawProfile)
	if err != nil {
		return Summary{}, gzipCompressed, fmt.Errorf("open profile stream: %w", err)
	}
	defer reader.Close()

	summary, err := trace_events.Summarize(reader, topN)
	if err != nil {
		if strings.HasSuffix(strings.ToLower(profileName), ".pb.gz") {
			return Summary{}, gzipCompressed, fmt.Errorf("profile %q is protobuf trace format; JSON trace parsing is not yet implemented", profileName)
		}
		return Summary{}, gzipCompressed, fmt.Errorf("decode trace events from %q: %w", profileName, err)
	}
	return summary, gzipCompressed, nil
}

func maybeDecompressGzip(data []byte) (io.ReadCloser, bool, error) {
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, true, err
		}
		return reader, true, nil
	}
	return io.NopCloser(bytes.NewReader(data)), false, nil
}
