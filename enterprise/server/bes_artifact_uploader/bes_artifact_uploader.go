package bes_artifact_uploader

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"golang.org/x/sync/errgroup"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type Result struct {
	Duration time.Duration
	Digest   *repb.Digest
	Path     string
	Name     string
	Err      error
}

// Uploader can be used to asynchronously upload artifacts associated with a
// build event stream.
type Uploader struct {
	ctx context.Context
	eg  *errgroup.Group
	// ByteStream client used to upload artifacts.
	bsClient bspb.ByteStreamClient
	// Publisher used to publish NamedSetOfFiles events when background uploads
	// have completed.
	bep *build_event_publisher.Publisher
	// Bytestream server host name and port.
	bytestreamHost string
	// Remote instance name to be used for uploaded artifacts.
	instanceName string
	// Channel where upload results are published (non-blocking).
	resultsChan chan *Result
}

func New(ctx context.Context, bsClient bspb.ByteStreamClient, bep *build_event_publisher.Publisher, bytestreamHost, instanceName string) *Uploader {
	eg, ctx := errgroup.WithContext(ctx)
	return &Uploader{
		ctx:            ctx,
		eg:             eg,
		bsClient:       bsClient,
		bep:            bep,
		bytestreamHost: bytestreamHost,
		instanceName:   instanceName,
		resultsChan:    make(chan *Result, 128),
	}
}

// Results returns a channel where upload results are published.
func (u *Uploader) Results() <-chan *Result {
	return u.resultsChan
}

// UploadFile enqueues a local file to be uploaded.
func (u *Uploader) UploadFile(path, name string) {
	u.eg.Go(func() (err error) {
		start := time.Now()
		result := &Result{Path: path, Name: name}
		defer func() {
			result.Err = err
			result.Duration = time.Since(start)
			select {
			case u.resultsChan <- result:
			default:
			}
		}()
		d, err := cachetools.UploadFile(u.ctx, u.bsClient, u.instanceName, repb.DigestFunction_SHA256, path)
		if err != nil {
			return err
		}
		result.Digest = d
		instanceNamePart := u.instanceName
		if instanceNamePart != "" {
			instanceNamePart += "/"
		}
		rn := digest.NewResourceName(d, u.instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
		rnString, err := rn.DownloadString()
		if err != nil {
			return err
		}
		uri := fmt.Sprintf("bytestream://%s/%s", u.bytestreamHost, rnString)
		return u.bep.Publish(&bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_NamedSet{
				NamedSet: &bespb.BuildEventId_NamedSetOfFilesId{Id: name},
			}},
			Payload: &bespb.BuildEvent_NamedSetOfFiles{NamedSetOfFiles: &bespb.NamedSetOfFiles{
				Files: []*bespb.File{
					{Name: name, File: &bespb.File_Uri{Uri: uri}},
				},
			}},
		})
	})
}

// Wait waits for all background uploads to complete.
// No new uploads should be enqueued after this is called.
func (u *Uploader) Wait() error {
	defer close(u.resultsChan)
	return u.eg.Wait()
}
