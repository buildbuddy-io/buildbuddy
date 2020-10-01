package byte_stream_server

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/test_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"

	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

func runByteStreamServer(ctx context.Context, env *test_environment.TestEnv, t *testing.T) *grpc.ClientConn {
	byteStreamServer, err := NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc := env.LocalGRPCServer()
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

	go runFunc()

	clientConn, err := env.LocalGRPCConn(ctx)
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

func readBlob(ctx context.Context, bsClient bspb.ByteStreamClient, d *digest.InstanceNameDigest, out io.Writer) error {
	req := &bspb.ReadRequest{
		ResourceName: digest.DownloadResourceName(d.Digest, d.GetInstanceName()),
		ReadOffset:   0,
		ReadLimit:    d.GetSizeBytes(),
	}
	stream, err := bsClient.Read(ctx, req)
	if err != nil {
		return err
	}

	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		out.Write(rsp.Data)
	}
	return nil
}

func TestRPCRead(t *testing.T) {
	ctx := context.Background()
	te, err := test_environment.GetTestEnv()
	if err != nil {
		t.Error(err)
	}
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	randStr := func(i int) string {
		rstr, err := random.RandomString(i)
		if err != nil {
			t.Error(err)
		}
		return rstr
	}
	cases := []struct {
		instanceNameDigest *digest.InstanceNameDigest
		wantData           string
		wantError          error
	}{
		{ // Simple Read
			instanceNameDigest: digest.NewInstanceNameDigest(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, ""),
			wantData:  randStr(1234),
			wantError: nil,
		},
		{ // Large Read
			instanceNameDigest: digest.NewInstanceNameDigest(&repb.Digest{
				Hash:      "ffd14ebb6c1b2701ac793ea1aff6dddf8540e734bd6d051ac2a24aa3ec062781",
				SizeBytes: 1000 * 1000 * 100,
			}, ""),
			wantData:  randStr(1000 * 1000 * 100),
			wantError: nil,
		},
	}

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	for _, tc := range cases {
		// Set the value in the cache.
		if err := te.GetCache().Set(ctx, tc.instanceNameDigest.Digest, []byte(tc.wantData)); err != nil {
			t.Fatal(err)
		}

		// Now read it back with the bytestream API.
		var buf bytes.Buffer
		gotErr := readBlob(ctx, bsClient, tc.instanceNameDigest, &buf)
		if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
			t.Errorf("got %v; want %v", gotErr, tc.wantError)
			//			continue
		}
		got := string(buf.Bytes())
		if got != tc.wantData {
			t.Errorf("got %.100s; want %.100s", got, tc.wantData)
		}
	}
}
