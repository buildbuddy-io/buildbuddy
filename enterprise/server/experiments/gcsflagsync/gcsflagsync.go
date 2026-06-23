// Package gcsflagsync provides a flagd sync.ISync implementation that sources
// the flag configuration from a GCS object, polling for changes at a fixed
// interval. It is meant to be passed to experiments.RegisterInProcessSync so a
// binary can evaluate experiment flags in-process without running a separate
// flagd backend.
package gcsflagsync

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	flagdsync "github.com/open-feature/flagd/core/pkg/sync"
)

var (
	bucket          = flag.String("experiments.gcs.bucket", "", "If set, sync the flagd flag configuration in-process from this GCS bucket instead of connecting to a flagd backend.")
	object          = flag.String("experiments.gcs.object", "", "Path to the flagd JSON config object within --experiments.gcs.bucket.")
	credentialsFile = flag.String("experiments.gcs.credentials_file", "", "Path to a JSON credentials file used to authenticate to GCS.")
	credentials     = flag.String("experiments.gcs.credentials", "", "JSON credentials used to authenticate to GCS.", flag.Secret)
	projectID       = flag.String("experiments.gcs.project_id", "", "GCP project ID owning the GCS bucket.")
	pollInterval    = flag.Duration("experiments.gcs.poll_interval", 10*time.Second, "How often to poll GCS for flag configuration changes.")
)

// New constructs a flagd sync.ISync that polls the configured GCS object for
// flag configuration changes. It returns an error if no bucket/object is
// configured.
func New(ctx context.Context) (flagdsync.ISync, error) {
	if *bucket == "" {
		return nil, status.FailedPreconditionError("experiments.gcs.bucket must be set")
	}
	if *object == "" {
		return nil, status.FailedPreconditionError("experiments.gcs.object must be set when experiments.gcs.bucket is set")
	}

	bs, err := gcs.NewGCSBlobStore(ctx, *bucket, *credentialsFile, *credentials, *projectID, false /*=enableCompression*/)
	if err != nil {
		return nil, err
	}
	return &syncer{
		blobstore: bs,
		object:    *object,
		interval:  *pollInterval,
	}, nil
}

// syncer implements flagd's sync.ISync by polling a flag configuration object
// from GCS at a fixed interval, pushing the latest contents to the in-process
// flagd resolver whenever they change. It is a lightweight alternative to
// running a separate flagd backend: the only "backend" is a JSON object in a
// bucket.
type syncer struct {
	blobstore interfaces.Blobstore
	object    string
	interval  time.Duration

	mu       sync.Mutex
	ready    bool
	lastData []byte
}

func (s *syncer) Init(ctx context.Context) error { return nil }

func (s *syncer) IsReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready
}

// Sync performs an initial fetch (so the resolver starts with flags) and then
// polls for changes until ctx is cancelled. A failed initial fetch is fatal so
// the provider doesn't come up serving an empty flag set.
func (s *syncer) Sync(ctx context.Context, dataSync chan<- flagdsync.DataSync) error {
	if err := s.fetch(ctx, dataSync); err != nil {
		return status.WrapError(err, "initial GCS flag sync")
	}
	s.mu.Lock()
	s.ready = true
	s.mu.Unlock()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.fetch(ctx, dataSync); err != nil {
				// A transient read failure just means we keep serving the last
				// known flags until the next tick succeeds.
				log.Warningf("experiments: GCS flag sync failed: %s", err)
			}
		}
	}
}

// ReSync re-pushes the current flag configuration. flagd calls this to recover
// the full config; we force a fresh fetch.
func (s *syncer) ReSync(ctx context.Context, dataSync chan<- flagdsync.DataSync) error {
	return s.fetch(ctx, dataSync)
}

// fetch reads the flag object and, if it changed since the last push, sends it
// to dataSync. Unchanged objects are skipped so flagd doesn't re-parse and emit
// change events on every poll.
func (s *syncer) fetch(ctx context.Context, dataSync chan<- flagdsync.DataSync) error {
	data, err := s.blobstore.ReadBlob(ctx, s.object)
	if err != nil {
		return err
	}
	s.mu.Lock()
	changed := !bytes.Equal(data, s.lastData)
	if changed {
		s.lastData = data
	}
	s.mu.Unlock()
	if !changed {
		return nil
	}
	select {
	case dataSync <- flagdsync.DataSync{FlagData: string(data), Source: s.object}:
	case <-ctx.Done():
	}
	return nil
}
