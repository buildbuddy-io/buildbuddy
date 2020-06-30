package byte_stream_server

import (
	"bytes"
	"context"
	"io"
	"net"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/test_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"regexp"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

var (
	lis *bufconn.Listener
)

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func localGRPCServer() *grpc.Server {
	lis = bufconn.Listen(1024 * 1024)
	return grpc.NewServer()
}

func localGRPCConn(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
}

// This is an example of a pretty simple go unit test: Tests are typically
// written in a "table-driven" way -- where you enumerate a list of expected
// inputs and outputs, often in an anonymous struct, and then exercise a method
// under test across those cases.
func TestExtractDigest(t *testing.T) {
	cases := []struct {
		resourceName     string
		matcher          *regexp.Regexp
		wantInstanceName string
		wantDigest       *repb.Digest
		wantError        error
	}{
		{ // download, bad hash
			resourceName:     "my_instance_name/blobs/invalid_hash/1234",
			matcher:          downloadRegex,
			wantInstanceName: "",
			wantDigest:       nil,
			wantError:        status.InvalidArgumentError(""),
		},
		{ // download, missing size
			resourceName:     "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/",
			matcher:          downloadRegex,
			wantInstanceName: "",
			wantDigest:       nil,
			wantError:        status.InvalidArgumentError(""),
		},
		{ // download, resource with instance name
			resourceName:     "my_instance_name/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:          downloadRegex,
			wantInstanceName: "my_instance_name",
			wantDigest:       &repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234},
			wantError:        nil,
		},
		{ // download, resource without instance name
			resourceName:     "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:          downloadRegex,
			wantInstanceName: "",
			wantDigest:       &repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234},
			wantError:        nil,
		},
		{ // upload, instance name
			resourceName:     "instance_name/uploads/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:          uploadRegex,
			wantInstanceName: "instance_name",
			wantDigest:       &repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234},
			wantError:        nil,
		},
		{ // upload, UUID
			resourceName:     "/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:          uploadRegex,
			wantInstanceName: "instance_name",
			wantDigest:       &repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234},
			wantError:        nil,
		},
		{ // upload, UUID and instance name
			resourceName:     "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:          uploadRegex,
			wantInstanceName: "instance_name",
			wantDigest:       &repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234},
			wantError:        nil,
		},
	}
	for _, tc := range cases {
		gotInstanceName, gotDigest, gotErr := extractDigest(tc.resourceName, downloadRegex)
		if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
			t.Errorf("got %v; want %v", gotErr, tc.wantError)
		} else {
			// We don't care about returned values in the error
			// case.
			continue
		}
		if gotInstanceName != tc.wantInstanceName {
			t.Errorf("got %v; want %v", gotInstanceName, tc.wantInstanceName)
		}
		if gotDigest.GetHash() != tc.wantDigest.GetHash() || gotDigest.GetSizeBytes() != tc.wantDigest.GetSizeBytes() {
			t.Errorf("got %v; want %v", gotDigest, tc.wantDigest)
		}
	}
}

func setupByteStreamServer(ctx context.Context, env *test_environment.TestEnv, t *testing.T) (func(), *grpc.ClientConn) {
	byteStreamServer, err := NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer := localGRPCServer()
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

	clientConn, err := localGRPCConn(ctx)
	if err != nil {
		t.Error(err)
	}

	runFn := func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Error(err)
		}
	}
	return runFn, clientConn
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
	runFn, clientConn := setupByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)
	go runFn()

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
		{
			instanceNameDigest: digest.NewInstanceNameDigest(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, ""),
			wantData:  randStr(1234),
			wantError: nil,
		},
	}

	ctx = perms.AttachUserPrefixToContext(ctx, te)
	for _, tc := range cases {
		// Set the value in the cache.
		if err := te.GetDigestCache().Set(ctx, tc.instanceNameDigest.Digest, []byte(tc.wantData)); err != nil {
			t.Error(err)
		}

		// Now read it back with the bytestream API.
		var got bytes.Buffer
		gotErr := readBlob(ctx, bsClient, tc.instanceNameDigest, &got)
		if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
			t.Errorf("got %v; want %v", gotErr, tc.wantError)
		} else {
			// We don't care about returned values in the error
			// case.
			continue
		}
		if string(got.Bytes()) != tc.wantData {
			t.Errorf("got %v; want %v", got.Bytes(), tc.wantData)
		}
	}
}
