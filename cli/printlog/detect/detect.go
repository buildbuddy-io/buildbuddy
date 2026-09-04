// Package detect identifies Bazel log formats supported by bb print.
package detect

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	rlpb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution_log"
	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

// Format is a log file format that `bb print` knows how to print.
type Format string

const (
	GRPCLog             Format = "grpc_log"
	CompactExecutionLog Format = "compact_execution_log"
)

type candidate struct {
	format            Format
	compressed        bool
	newMessage        func() proto.Message
	hasRequiredFields func(proto.Message) bool
}

var candidates = []candidate{
	{
		format:            GRPCLog,
		compressed:        false,
		newMessage:        func() proto.Message { return &rlpb.LogEntry{} },
		hasRequiredFields: func(m proto.Message) bool { return strings.Contains(m.(*rlpb.LogEntry).GetMethodName(), "/") },
	},
	{
		format:            CompactExecutionLog,
		compressed:        true,
		newMessage:        func() proto.Message { return &spb.ExecLogEntry{} },
		hasRequiredFields: func(m proto.Message) bool { return m.(*spb.ExecLogEntry).GetType() != nil },
	},
}

// FileFormat returns the format of the log file at path.
func FileFormat(path string) (Format, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return StreamFormat(f)
}

// StreamFormat returns the format of a log stream.
func StreamFormat(r io.Reader) (Format, error) {
	frames, compressed, err := sample(r)
	if err != nil {
		return "", err
	}
	for _, c := range candidates {
		if c.compressed == compressed && c.matches(frames) {
			return c.format, nil
		}
	}
	return "", errors.New("unsupported log format (expected output from --remote_grpc_log or --execution_log_compact_file)")
}

// sample returns a bounded prefix of frames and whether r is zstd-compressed.
func sample(r io.Reader) (frames [][]byte, compressed bool, err error) {
	const (
		zstdMagic    = "\x28\xb5\x2f\xfd"
		sampleFrames = 8
		sampleBytes  = 1 << 20
		maxFrameSize = 256 << 20
	)
	br := bufio.NewReader(r)
	magic, err := br.Peek(len(zstdMagic))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false, err
	}
	framed := br
	if string(magic) == zstdMagic {
		compressed = true
		// Decode serially; the default reads ~3.6x further ahead than we sample.
		zr, err := zstd.NewReader(br, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, true, err
		}
		defer zr.Close()
		framed = bufio.NewReader(zr)
	}

	bytesRead := 0
	for len(frames) < sampleFrames {
		size, err := binary.ReadUvarint(framed)
		if err != nil || size > maxFrameSize {
			break
		}
		if len(frames) > 0 && bytesRead+int(size) > sampleBytes {
			break
		}
		// Grow with the bytes actually read. The length prefix is untrusted, so
		// allocating it up front lets a few bytes of garbage ask for maxFrameSize.
		var frame bytes.Buffer
		if _, err := io.CopyN(&frame, framed, int64(size)); err != nil {
			break
		}
		frames = append(frames, frame.Bytes())
		bytesRead += int(size)
	}
	return frames, compressed, nil
}

// matches reports whether the sampled messages identify c's format.
func (c candidate) matches(frames [][]byte) bool {
	sampled, accounted, leading := 0, 0, true
	for _, frame := range frames {
		if len(frame) == 0 {
			continue
		}
		sampled++
		m := c.newMessage()
		err := proto.Unmarshal(frame, m)
		if leading {
			// The first entry pins the format. Bazel writes a recognizable one at
			// the head of both logs, and checking only it leaves room for later
			// entries a newer Bazel added.
			if err != nil || !c.hasRequiredFields(m) {
				return false
			}
			leading = false
		}
		if err == nil && unknownBytes(m.ProtoReflect())*10 <= len(frame) {
			accounted++
		}
	}
	// Tolerate a minority of entries the vendored protos don't know: an
	// ExecLogEntry oneof case added by a newer Bazel parses as entirely unknown
	// bytes, and shouldn't sink an otherwise clean sample.
	return sampled > 0 && accounted*2 > sampled
}

// unknownBytes returns the encoded size of unknown fields in m and nested messages.
func unknownBytes(m protoreflect.Message) int {
	n := len(m.GetUnknown())
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			if isMessage(fd.MapValue()) {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					n += unknownBytes(mv.Message())
					return true
				})
			}
		case fd.IsList():
			if isMessage(fd) {
				l := v.List()
				for i := range l.Len() {
					n += unknownBytes(l.Get(i).Message())
				}
			}
		case isMessage(fd):
			n += unknownBytes(v.Message())
		}
		return true
	})
	return n
}

// isMessage reports whether fd can contain a message value.
func isMessage(fd protoreflect.FieldDescriptor) bool {
	return fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind
}
