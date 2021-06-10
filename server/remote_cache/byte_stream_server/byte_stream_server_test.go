package byte_stream_server

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

func runByteStreamServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
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
	te := testenv.GetTestEnv(t)
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
		wantError          error
		instanceNameDigest *digest.InstanceNameDigest
		wantData           string
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
		{ // 0 length read
			instanceNameDigest: digest.NewInstanceNameDigest(&repb.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}, ""),
			wantData:  "",
			wantError: nil,
		},
	}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
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

func TestRPCWrite(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Test that a regular bytestream upload works.
	d, readSeeker := testdigest.NewRandomDigestReader(t, 1000)
	instanceNameDigest := digest.NewInstanceNameDigest(d, "")
	_, err := cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRPCMalformedWrite(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Test that a malformed upload (incorrect digest) is rejected.
	d, buf := testdigest.NewRandomDigestBuf(t, 1000)
	instanceNameDigest := digest.NewInstanceNameDigest(d, "")
	buf[0] = ^buf[0] // flip bits in byte to corrupt digest.

	readSeeker := bytes.NewReader(buf)
	_, err := cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if !status.IsDataLossError(err) {
		t.Fatalf("Expected data loss error but got %s", err)
	}
}

func TestRPCTooLongWrite(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Test that a malformed upload (wrong bytesize) is rejected.
	d, buf := testdigest.NewRandomDigestBuf(t, 1000)
	d.SizeBytes += 1 // increment expected byte count by 1 to trigger mismatch.
	instanceNameDigest := digest.NewInstanceNameDigest(d, "")

	readSeeker := bytes.NewReader(buf)
	_, err := cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if !status.IsDataLossError(err) {
		t.Fatalf("Expected data loss error but got %s", err)
	}
}
