package download

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
)

func TestArtifactOutputPaths(t *testing.T) {
	outputDir := t.TempDir()
	paths, skipped := artifactOutputPaths([]*apipb.File{
		{Name: "result.patch", Uri: "bytestream://cache/blobs/aaa/1"},
		// Duplicate artifacts with the same name should be skipped.
		{Name: "result.patch", Uri: "bytestream://cache/blobs/bbb/1"},
		{Name: "nested/artifact.txt"},
		// Invalid artifacts with invalid paths should be skipped.
		{Name: "../file"},
		{Name: "/absolute/file"},
	}, outputDir)
	require.Equal(t, 3, skipped)
	want := []string{
		filepath.Join(outputDir, "result.patch"),
		"",
		filepath.Join(outputDir, "nested", "artifact.txt"),
		"",
		"",
	}
	require.Equal(t, want, paths)
}

type fakeGetFileStream struct {
	apipb.ApiService_GetFileClient
	chunks [][]byte
	err    error
}

func (s *fakeGetFileStream) Recv() (*apipb.GetFileResponse, error) {
	if len(s.chunks) > 0 {
		chunk := s.chunks[0]
		s.chunks = s.chunks[1:]
		return &apipb.GetFileResponse{Data: chunk}, nil
	}
	if s.err != nil {
		return nil, s.err
	}
	return nil, io.EOF
}

type fakeAPIClient struct {
	apipb.ApiServiceClient
	getFileCalls int
}

func (c *fakeAPIClient) GetInvocation(ctx context.Context, req *apipb.GetInvocationRequest, opts ...grpc.CallOption) (*apipb.GetInvocationResponse, error) {
	return &apipb.GetInvocationResponse{Invocation: []*apipb.Invocation{{
		Artifacts: []*apipb.File{{Name: "out.txt", Uri: "bytestream://cache/blobs/aaa/11"}},
	}}}, nil
}

func (c *fakeAPIClient) GetFile(ctx context.Context, req *apipb.GetFileRequest, opts ...grpc.CallOption) (apipb.ApiService_GetFileClient, error) {
	c.getFileCalls++
	if c.getFileCalls == 1 {
		return &fakeGetFileStream{chunks: [][]byte{[]byte("partial")}, err: status.UnavailableError("connection reset")}, nil
	}
	return &fakeGetFileStream{chunks: [][]byte{[]byte("hello "), []byte("world")}}, nil
}

func TestDownloadArtifactsRetriesGetFile(t *testing.T) {
	outputDir := t.TempDir()
	client := &fakeAPIClient{}
	err := downloadArtifacts(context.Background(), client, "inv", outputDir)
	require.NoError(t, err)
	require.Equal(t, 2, client.getFileCalls)
	b, err := os.ReadFile(filepath.Join(outputDir, "out.txt"))
	require.NoError(t, err)
	require.Equal(t, "hello world", string(b))
}
