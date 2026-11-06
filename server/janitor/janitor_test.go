package janitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"
)

type invocationDBStub struct {
	interfaces.InvocationDB
	expired        []*tables.Invocation
	lookupErr      error
	deletedBatches [][]string
	events         *[]string
}

func (s *invocationDBStub) LookupExpiredInvocations(ctx context.Context, cutoff time.Time, limit int) ([]*tables.Invocation, error) {
	return s.expired, s.lookupErr
}

func (s *invocationDBStub) DeleteInvocations(ctx context.Context, invocationIDs []string) error {
	s.deletedBatches = append(s.deletedBatches, append([]string(nil), invocationIDs...))
	*s.events = append(*s.events, "delete invocations")
	return nil
}

type blobstoreStub struct {
	interfaces.Blobstore
	deletedBlobs []string
	errs         map[string]error
	events       *[]string
}

func (s *blobstoreStub) DeleteBlob(ctx context.Context, blobName string) error {
	s.deletedBlobs = append(s.deletedBlobs, blobName)
	*s.events = append(*s.events, "delete "+blobName)
	return s.errs[blobName]
}

func TestDeleteExpiredInvocationsDeletesBatchAfterAllBlobs(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var events []string
	idb := &invocationDBStub{expired: []*tables.Invocation{
		{InvocationID: "invocation-1", BlobID: "blob-1"},
		{InvocationID: "invocation-2", BlobID: "blob-2"},
	}, events: &events}
	bs := &blobstoreStub{errs: map[string]error{"blob-1": errors.New("blob deletion failed")}, events: &events}
	env.SetInvocationDB(idb)
	env.SetBlobstore(bs)

	deleteExpiredInvocations(&JanitorConfig{env: env, ttl: time.Hour, batchSize: 10})

	require.Equal(t, []string{"blob-1", "blob-2"}, bs.deletedBlobs)
	require.Equal(t, [][]string{{"invocation-1", "invocation-2"}}, idb.deletedBatches)
	require.Equal(t, []string{"delete blob-1", "delete blob-2", "delete invocations"}, events)
}

func TestDeleteExpiredInvocationsDoesNotDeleteAnEmptyOrFailedLookup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		expired   []*tables.Invocation
		lookupErr error
	}{
		{name: "empty"},
		{name: "lookup error", lookupErr: errors.New("lookup failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			var events []string
			idb := &invocationDBStub{expired: tc.expired, lookupErr: tc.lookupErr, events: &events}
			bs := &blobstoreStub{events: &events}
			env.SetInvocationDB(idb)
			env.SetBlobstore(bs)

			deleteExpiredInvocations(&JanitorConfig{env: env, ttl: time.Hour, batchSize: 10})

			require.Empty(t, bs.deletedBlobs)
			require.Empty(t, idb.deletedBatches)
		})
	}
}
