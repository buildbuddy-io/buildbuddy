package bigcache_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/bigcache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/mockmetadata"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockgcs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	mdspb "github.com/buildbuddy-io/buildbuddy/proto/metadata_service"
)

const MB = 1_000_000

var emptyUserMap = testauth.TestUsers()

func getAnonContext(t testing.TB, env environment.Env) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
	require.NoError(t, err)
	return ctx
}

func TestReadWrite(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB

	testSizes := []int64{
		1, 10, 100, 256, 512, 1000, 1024, 2 * 1024, 10000, 1000000,
	}

	gcsTTLDays := int64(1)
	clock := clockwork.NewFakeClock()
	mockGCS := mockgcs.New(clock)
	mockGCS.SetBucketCustomTimeTTL(ctx, gcsTTLDays)
	fileStorer := filestore.New(filestore.WithGCSBlobstore(mockGCS, "one-bucket"))

	mm, err := mockmetadata.NewServer(1000, fileStorer)
	require.NoError(t, err)

	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	mdspb.RegisterMetadataServiceServer(te.GetGRPCServer(), mm)
	conn, err := testenv.LocalGRPCConn(
		te.GetServerContext(),
		lis,
		interceptors.GetUnaryClientIdentityInterceptor(te),
		interceptors.GetStreamClientIdentityInterceptor(te),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	go runServer()

	options := bigcache.Options{
		Name:           "TestReadWrite",
		Clock:          clock,
		FileStorer:     fileStorer,
		MetadataClient: mdspb.NewMetadataServiceClient(conn),

		MaxInlineFileSizeBytes:      1000,
		MinBytesAutoZstdCompression: 100,

		GCSTTLDays: 1,
		Partitions: []disk.Partition{{
			ID:           "default",
			MaxSizeBytes: maxSizeBytes,
		}},
	}
	bc, err := bigcache.New(te, options)
	require.NoError(t, err)

	for _, testSize := range testSizes {
		desc := fmt.Sprintf("size_%d", testSize)
		t.Run(desc, func(t *testing.T) {
			rn, buf := testdigest.RandomCASResourceBuf(t, testSize)
			// Use Writer() to set the bytes in the cache.
			wc, err := bc.Writer(ctx, rn)
			require.NoError(t, err, "Error getting %q writer", rn.GetDigest().GetHash())
			_, err = wc.Write(buf)
			require.NoError(t, err)
			err = wc.Commit()
			require.NoError(t, err)
			err = wc.Close()
			require.NoError(t, err)

			// Use Reader() to get the bytes from the cache.
			reader, err := bc.Reader(ctx, rn, 0, 0)
			require.NoError(t, err, "Error getting %q reader", rn.GetDigest().GetHash())
			d2 := testdigest.ReadDigestAndClose(t, reader)
			require.Equal(t, rn.GetDigest().GetHash(), d2.GetHash())
		})
	}

}
