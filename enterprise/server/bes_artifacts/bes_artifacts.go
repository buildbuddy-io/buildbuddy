package bes_artifacts

import (
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"path/filepath"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// Result contains info about a completed attempt at uploading a file to the
// cache and publishing it to the BES as a NamedSet.
type Result struct {
	Duration   time.Duration
	Digest     *repb.Digest
	NamedSetID string
	Path       string
	Name       string
	Err        error
}

// Uploader can be used to asynchronously upload artifacts associated with a
// build event stream.
type Uploader struct {
	ctx  context.Context
	eg   *errgroup.Group
	conn *grpc_client.ClientConnPool
	// ByteStream client used to upload artifacts.
	bsClient bspb.ByteStreamClient
	// Publisher used to publish NamedSetOfFiles events when background uploads
	// have completed.
	bep *build_event_publisher.Publisher
	// Bytestream URI prefix (like "bytestream://remote.buildbuddy.io:443")
	bytestreamURIPrefix string
	// Remote instance name to be used for uploaded artifacts.
	instanceName string

	resultsMu sync.Mutex
	// Array containing all upload results.
	results []*Result
}

// NewUploader returns a new Uploader instance. The caller must call Wait to
// close the connection that is opened by this function.
func NewUploader(ctx context.Context, bep *build_event_publisher.Publisher, cacheTarget, instanceName string) (*Uploader, error) {
	eg, ctx := errgroup.WithContext(ctx)

	u, err := url.Parse(cacheTarget)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to parse cache target %q: %s", cacheTarget, err)
	}
	bytestreamURIPrefix := "bytestream://" + u.Host

	conn, err := grpc_client.DialSimple(cacheTarget)
	if err != nil {
		return nil, status.WrapErrorf(err, "dial %q", cacheTarget)
	}
	return &Uploader{
		ctx:                 ctx,
		eg:                  eg,
		conn:                conn,
		bsClient:            bspb.NewByteStreamClient(conn),
		bep:                 bep,
		bytestreamURIPrefix: bytestreamURIPrefix,
		instanceName:        instanceName,
	}, nil
}

// UploadDirectory recursively uploads all files in a directory as a
// NamedSetOfFiles in the background. File names are computed as their path
// relative to the directory.
func (u *Uploader) UploadDirectory(namedSetID, root string) {
	u.eg.Go(func() error {
		return u.uploadDirectory(namedSetID, root)
	})
}

func (u *Uploader) uploadDirectory(namedSetID, root string) error {
	var uploadChans []chan *Result
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if !d.Type().IsRegular() {
			return nil
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		ch := u.uploadFile(namedSetID, path, name)
		uploadChans = append(uploadChans, ch)
		return nil
	})
	if err != nil {
		return err
	}

	var files []*bespb.File
	for _, uploadChan := range uploadChans {
		r := <-uploadChan
		rn := digest.NewResourceName(r.Digest, u.instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
		rnString, err := rn.DownloadString()
		if err != nil {
			return err
		}
		uri := fmt.Sprintf("%s/%s", u.bytestreamURIPrefix, rnString)
		f := &bespb.File{
			Name:   r.Name,
			File:   &bespb.File_Uri{Uri: uri},
			Digest: rn.GetDigest().GetHash(),
			Length: rn.GetDigest().GetSizeBytes(),
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		// No artifacts uploaded; don't publish an unnecessary event
		return nil
	}
	return u.bep.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_NamedSet{
			NamedSet: &bespb.BuildEventId_NamedSetOfFilesId{Id: namedSetID},
		}},
		Payload: &bespb.BuildEvent_NamedSetOfFiles{NamedSetOfFiles: &bespb.NamedSetOfFiles{
			Files: files,
		}},
	})
}

// uploadFile starts a background file upload.
func (u *Uploader) uploadFile(setID, path, name string) chan *Result {
	ch := make(chan *Result, 1)
	u.eg.Go(func() error {
		start := time.Now()
		result := &Result{
			NamedSetID: setID,
			Name:       name,
			Path:       path,
		}
		result.Digest, result.Err = cachetools.UploadFile(u.ctx, u.bsClient, u.instanceName, repb.DigestFunction_SHA256, path)
		result.Duration = time.Since(start)
		ch <- result

		u.resultsMu.Lock()
		u.results = append(u.results, result)
		u.resultsMu.Unlock()

		return result.Err
	})
	return ch
}

// Wait waits for all background uploads to complete, and returns metadata
// for all uploads.
// No new uploads should be enqueued after this is called.
func (u *Uploader) Wait() ([]*Result, error) {
	err := u.eg.Wait()
	_ = u.conn.Close()
	return u.results, err
}
