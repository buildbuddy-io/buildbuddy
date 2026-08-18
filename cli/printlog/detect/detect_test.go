package detect_test

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/cli/printlog/detect"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rlpb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution_log"
	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

var entryForFormat = map[detect.Format]func(int) proto.Message{
	detect.GRPCLog: func(i int) proto.Message {
		return &rlpb.LogEntry{
			MethodName: "build.bazel.remote.execution.v2.ActionCache/GetActionResult",
			Metadata: &repb.RequestMetadata{
				ToolDetails:             &repb.ToolDetails{ToolName: "bazel", ToolVersion: "9.2.0"},
				ActionId:                fmt.Sprintf("%064x", i*2654435761),
				ToolInvocationId:        "66f29b0e-f165-4b0a-b9ee-dafff3263a03",
				CorrelatedInvocationsId: "208e9d34-a0d3-464a-a51a-4d1f32a024c3",
			},
			Status:    &statuspb.Status{Code: int32(codes.NotFound), Message: "not found"},
			StartTime: timestamppb.New(time.Unix(1700000000, int64(i))),
			EndTime:   timestamppb.New(time.Unix(1700000001, int64(i))),
			Details: &rlpb.RpcCallDetails{Details: &rlpb.RpcCallDetails_GetActionResult{
				GetActionResult: &rlpb.GetActionResultDetails{
					Request: &repb.GetActionResultRequest{
						InstanceName: "",
						ActionDigest: &repb.Digest{Hash: fmt.Sprintf("%064x", i), SizeBytes: int64(i)},
					},
				},
			}},
		}
	},
	detect.CompactExecutionLog: func(i int) proto.Message {
		if i == 0 {
			return &spb.ExecLogEntry{
				Type: &spb.ExecLogEntry_Invocation_{
					Invocation: &spb.ExecLogEntry_Invocation{
						HashFunctionName:           "SHA-256",
						WorkspaceRunfilesDirectory: "_main",
					},
				},
			}
		}
		return &spb.ExecLogEntry{
			Id: uint32(i),
			Type: &spb.ExecLogEntry_File_{
				File: &spb.ExecLogEntry_File{
					Path:   fmt.Sprintf("bazel-out/k8-fastbuild/bin/pkg%d/file%d.o", i%977, i),
					Digest: &spb.Digest{Hash: fmt.Sprintf("%064x", i*2654435761), SizeBytes: int64(i)},
				},
			},
		}
	},
}

func delimited(t *testing.T, format detect.Format, n int) []byte {
	t.Helper()
	var buf bytes.Buffer
	for i := range n {
		_, err := protodelim.MarshalTo(&buf, entryForFormat[format](i))
		require.NoError(t, err)
	}
	return buf.Bytes()
}

func zstdCompress(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = w.Write(b)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// logFile renders a log the way Bazel writes it, compressing only the compact log.
func logFile(t *testing.T, format detect.Format, n int) []byte {
	t.Helper()
	b := delimited(t, format, n)
	if format == detect.CompactExecutionLog {
		return zstdCompress(t, b)
	}
	return b
}

func TestStreamFormat(t *testing.T) {
	for format := range entryForFormat {
		for _, n := range []int{1, 2, 50} {
			t.Run(fmt.Sprintf("%s/%d_entries", format, n), func(t *testing.T) {
				got, err := detect.StreamFormat(bytes.NewReader(logFile(t, format, n)))
				require.NoError(t, err)
				require.Equal(t, format, got)
			})
		}
	}
}

func binaryExecLog(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	for i := range 4 {
		_, err := protodelim.MarshalTo(&buf, &spb.SpawnExec{
			CommandArgs:          []string{"/usr/bin/gcc", "-c", fmt.Sprintf("file%d.c", i)},
			EnvironmentVariables: []*spb.EnvironmentVariable{{Name: "PATH", Value: "/usr/bin"}},
			Mnemonic:             "CppCompile",
			TargetLabel:          fmt.Sprintf("//pkg:file%d", i),
			ListedOutputs:        []string{fmt.Sprintf("bazel-out/k8-fastbuild/bin/file%d.o", i)},
			Runner:               "remote",
		})
		require.NoError(t, err)
	}
	return buf.Bytes()
}

func TestStreamFormatRejectsNonLogs(t *testing.T) {
	var unrelatedProto bytes.Buffer
	_, err := protodelim.MarshalTo(&unrelatedProto, &repb.Platform{
		Properties: []*repb.Platform_Property{{Name: "OSFamily", Value: "linux"}},
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		contents []byte
	}{
		{"empty", nil},
		{"json", []byte(`{"commandArgs": ["/usr/bin/gcc"], "mnemonic": "CppCompile"}`)},
		{"text", []byte("INFO: Elapsed time: 1.234s, Critical Path: 0.56s\n")},
		{"random_bytes", bytes.Repeat([]byte{0xFF, 0xFE, 0xFD, 0xFC}, 256)},
		{"unrelated_proto", unrelatedProto.Bytes()},
		{"binary_execution_log", binaryExecLog(t)},
		// Decodes to exactly maxFrameSize, in a 5 byte file.
		{"oversized_length_prefix", []byte{0x80, 0x80, 0x80, 0x80, 0x01}},
		// Bazel compresses the compact log and nothing else, and each printer
		// assumes as much, so a mismatch is deliberately not detected.
		{"compressed_grpc_log", zstdCompress(t, delimited(t, detect.GRPCLog, 4))},
		{"uncompressed_compact_log", delimited(t, detect.CompactExecutionLog, 4)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := detect.StreamFormat(bytes.NewReader(tc.contents))
			require.Error(t, err)
		})
	}
}

func TestStreamFormatToleratesUnknownEntryTypes(t *testing.T) {
	unknownEntry := protowire.AppendTag(nil, protowire.MaxValidNumber, protowire.BytesType)
	unknownEntry = protowire.AppendBytes(unknownEntry, []byte("abcd"))

	var buf bytes.Buffer
	writeEntry := func(i int) {
		_, err := protodelim.MarshalTo(&buf, entryForFormat[detect.CompactExecutionLog](i))
		require.NoError(t, err)
	}
	writeEntry(0) // Invocation
	for range 3 {
		buf.WriteByte(byte(len(unknownEntry)))
		buf.Write(unknownEntry)
	}
	for i := 1; i <= 4; i++ {
		writeEntry(i)
	}

	format, err := detect.StreamFormat(bytes.NewReader(zstdCompress(t, buf.Bytes())))
	require.NoError(t, err)
	require.Equal(t, detect.CompactExecutionLog, format)
}

func TestFileFormatOnRealCompactExecutionLogs(t *testing.T) {
	dir, err := runfiles.Rlocation("com_github_buildbuddy_io_buildbuddy/cli/explain/compactgraph/testdata")
	require.NoError(t, err)
	logs, err := filepath.Glob(filepath.Join(dir, "*", "*.pb.zstd"))
	require.NoError(t, err)
	require.NotEmpty(t, logs, "no real logs found to test against")

	for _, log := range logs {
		name := filepath.Join(filepath.Base(filepath.Dir(log)), filepath.Base(log))
		t.Run(name, func(t *testing.T) {
			format, err := detect.FileFormat(log)
			require.NoError(t, err)
			require.Equal(t, detect.CompactExecutionLog, format)
		})
	}
}

func TestStreamFormatReadsOnlyTheStart(t *testing.T) {
	for format := range entryForFormat {
		t.Run(string(format), func(t *testing.T) {
			contents := logFile(t, format, 50000)
			require.Greater(t, len(contents), 512<<10, "test log should be big enough to matter")

			r := &countingReader{Reader: bytes.NewReader(contents)}
			got, err := detect.StreamFormat(r)
			require.NoError(t, err)
			require.Equal(t, format, got)
			// Tight enough to catch the zstd decoder reading ahead, which it does
			// without WithDecoderConcurrency(1).
			require.Less(t, r.n, 32<<10, "detection read too much of the log")
		})
	}
}

type countingReader struct {
	io.Reader
	n int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += n
	return n, err
}
