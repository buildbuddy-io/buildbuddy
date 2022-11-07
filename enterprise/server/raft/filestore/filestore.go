// Package filestore implements io for reading bytestreams to/from pebble entries.
package filestore

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// returns partitionID, groupID, isolation, remote_instance_name, hash
// callers may choose to use all or some of these elements when constructing
// a file path or file key. because these strings may be persisted to disk, this
// function should rarely change and must be kept backwards compatible.
func fileRecordSegments(r *rfpb.FileRecord) (partID string, groupID string, isolation string, remoteInstanceHash string, digestHash string, err error) {
	if r.GetIsolation().GetPartitionId() == "" {
		err = status.FailedPreconditionError("Empty partition ID not allowed in filerecord.")
		return
	}
	partID = r.GetIsolation().GetPartitionId()
	groupID = r.GetIsolation().GetGroupId()

	if r.GetIsolation().GetCacheType() == resource.CacheType_CAS {
		isolation = "cas"
	} else if r.GetIsolation().GetCacheType() == resource.CacheType_AC {
		isolation = "ac"
		if remoteInstanceName := r.GetIsolation().GetRemoteInstanceName(); remoteInstanceName != "" {
			remoteInstanceHash = strconv.Itoa(int(crc32.ChecksumIEEE([]byte(remoteInstanceName))))
		}
	} else {
		err = status.FailedPreconditionError("Isolation type must be explicitly set, not UNKNOWN.")
		return
	}
	if len(r.GetDigest().GetHash()) <= 4 {
		err = status.FailedPreconditionError("Malformed digest; too short.")
		return
	}
	digestHash = r.GetDigest().GetHash()
	return
}

type Store interface {
	FileKey(r *rfpb.FileRecord) ([]byte, error)
	FilePath(fileDir string, f *rfpb.StorageMetadata_FileMetadata) string
	FileMetadataKey(r *rfpb.FileRecord) ([]byte, error)

	NewReader(ctx context.Context, fileDir string, md *rfpb.StorageMetadata, offset, limit int64) (io.ReadCloser, error)
	NewWriter(ctx context.Context, fileDir string, fileRecord *rfpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error)

	InlineReader(f *rfpb.StorageMetadata_InlineMetadata, offset, limit int64) (io.ReadCloser, error)
	InlineWriter(ctx context.Context, sizeBytes int64) interfaces.MetadataWriteCloser

	FileReader(ctx context.Context, fileDir string, f *rfpb.StorageMetadata_FileMetadata, offset, limit int64) (io.ReadCloser, error)
	FileWriter(ctx context.Context, fileDir string, fileRecord *rfpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error)

	DeleteStoredFile(ctx context.Context, fileDir string, md *rfpb.StorageMetadata) error
	FileExists(ctx context.Context, fileDir string, md *rfpb.StorageMetadata) bool

	LinkOrCopyFile(ctx context.Context, md *rfpb.StorageMetadata, srcFileDir, targetFileDir string) error
}

type fileStorer struct {
	isolateByGroupIDs           bool
	prioritizeHashInMetadataKey bool
}

type Opts struct {
	// IsolateByGroupIDs includes the caller group ID in the metadata and file
	// keys.
	IsolateByGroupIDs bool
	// PrioritizeHashInMetadataKey controls the placement of the digest within the metadata
	// key. When enabled, the digest is placed before the cache type & isolation
	// parts.
	PrioritizeHashInMetadataKey bool
}

// New creates a new filestorer interface.
func New(opts Opts) Store {
	return &fileStorer{
		isolateByGroupIDs:           opts.IsolateByGroupIDs,
		prioritizeHashInMetadataKey: opts.PrioritizeHashInMetadataKey,
	}
}

func (fs *fileStorer) FilePath(fileDir string, f *rfpb.StorageMetadata_FileMetadata) string {
	fp := f.GetFilename()
	if !filepath.IsAbs(fp) {
		fp = filepath.Join(fileDir, f.GetFilename())
	}
	return fp
}

// FileKey is the partial path where a file will be written.
// For example, given a fileRecord with FileKey: "foo/bar", the filestore will
// write the file at a path like "/root/dir/blobs/foo/bar".
func (fs *fileStorer) FileKey(r *rfpb.FileRecord) ([]byte, error) {
	// This function cannot change without a data migration.
	// filekeys look like this:
	//   // {partitionID}/{groupID}/{ac|cas}/{hashPrefix:4}/{hash}
	//   // for example:
	//   //   PART123/GR123/ac/44321/abcd/abcd12345asdasdasd123123123asdasdasd
	//   //   PART123/GR124/cas/abcd/abcd12345asdasdasd123123123asdasdasd
	partID, groupID, isolation, remoteInstanceHash, hash, err := fileRecordSegments(r)
	if err != nil {
		return nil, err
	}
	if fs.isolateByGroupIDs && r.GetIsolation().GetCacheType() == resource.CacheType_AC {
		return []byte(filepath.Join(partID, groupID, isolation, remoteInstanceHash, hash[:4], hash)), nil
	} else {
		return []byte(filepath.Join(partID, isolation, remoteInstanceHash, hash[:4], hash)), nil
	}
}

// FileMetadataKey is the partial key name where a file's metadata will be
// written in pebble.
// For example, given a fileRecord with FileMetadataKey: "baz/bap", the filestore will
// write the file's metadata under pebble key like:
//   - baz/bap
func (fs *fileStorer) FileMetadataKey(r *rfpb.FileRecord) ([]byte, error) {
	// This function cannot change without a data migration.
	//
	// Metadata keys look like this when PrioritizeHashInMetadataKey is off:
	//    {partID}/{groupID}/{ac|cas}/{hash}
	//    for example:
	//      PART123456/GR123/ac/44321/abcd12345asdasdasd123123123asdasdasd
	//      PART123456/GR123/cas/abcd12345asdasdasd123123123asdasdasd
	//
	// Metadata keys look like this when PrioritizeHashInMetadataKey is on:
	//    {partID}/{groupID}/{hash}/{ac|cas}
	//    for example:
	//      PART123456/GR123/abcd12345asdasdasd123123123asdasdasd/ac/44321
	//      PART123456/GR123/abcd12345asdasdasd123123123asdasdasd/cas
	partID, groupID, isolation, remoteInstanceHash, hash, err := fileRecordSegments(r)
	if err != nil {
		return nil, err
	}

	numParts := 4
	if fs.isolateByGroupIDs {
		numParts = 5
	}
	parts := make([]string, 0, numParts)
	parts = append(parts, partID)
	if fs.isolateByGroupIDs && r.GetIsolation().GetCacheType() == resource.CacheType_AC {
		parts = append(parts, groupID)
	}
	if fs.prioritizeHashInMetadataKey {
		parts = append(parts, hash)
	}
	parts = append(parts, isolation)
	parts = append(parts, remoteInstanceHash)
	if !fs.prioritizeHashInMetadataKey {
		parts = append(parts, hash)
	}

	return []byte(filepath.Join(parts...)), nil
}

func (fs *fileStorer) NewWriter(ctx context.Context, fileDir string, fileRecord *rfpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error) {
	// New files are written using this method. Existing files will be read
	// from wherever they were originally written according to their stored
	// StorageMetadata.
	return fs.FileWriter(ctx, fileDir, fileRecord)
}

func (fs *fileStorer) NewReader(ctx context.Context, fileDir string, md *rfpb.StorageMetadata, offset, limit int64) (io.ReadCloser, error) {
	switch {
	case md.GetFileMetadata() != nil:
		return fs.FileReader(ctx, fileDir, md.GetFileMetadata(), offset, limit)
	case md.GetInlineMetadata() != nil:
		return fs.InlineReader(md.GetInlineMetadata(), offset, limit)
	default:
		return nil, status.InvalidArgumentErrorf("No stored metadata: %+v", md)
	}
}

func (fs *fileStorer) InlineReader(f *rfpb.StorageMetadata_InlineMetadata, offset, limit int64) (io.ReadCloser, error) {
	r := bytes.NewReader(f.GetData())
	r.Seek(offset, 0)
	length := int64(len(f.GetData()))
	if limit != 0 && limit < length {
		length = limit
	}
	if length > 0 {
		return io.NopCloser(io.LimitReader(r, length)), nil
	}
	return io.NopCloser(r), nil
}

type inlineWriter struct {
	*bytes.Buffer
}

func (iw *inlineWriter) Close() error {
	return nil
}

func (iw *inlineWriter) Metadata() *rfpb.StorageMetadata {
	return &rfpb.StorageMetadata{
		InlineMetadata: &rfpb.StorageMetadata_InlineMetadata{
			Data:          iw.Buffer.Bytes(),
			CreatedAtNsec: time.Now().UnixNano(),
		},
	}
}

func (fs *fileStorer) InlineWriter(ctx context.Context, sizeBytes int64) interfaces.MetadataWriteCloser {
	return &inlineWriter{bytes.NewBuffer(make([]byte, 0, sizeBytes))}
}

type fileChunker struct {
	interfaces.CommittedWriteCloser
	fileName string
}

func (c *fileChunker) Metadata() *rfpb.StorageMetadata {
	return &rfpb.StorageMetadata{
		FileMetadata: &rfpb.StorageMetadata_FileMetadata{
			Filename: c.fileName,
		},
	}
}

func (fs *fileStorer) FileReader(ctx context.Context, fileDir string, f *rfpb.StorageMetadata_FileMetadata, offset, limit int64) (io.ReadCloser, error) {
	fp := fs.FilePath(fileDir, f)
	return disk.FileReader(ctx, fp, offset, limit)
}

func (fs *fileStorer) LinkOrCopyFile(ctx context.Context, md *rfpb.StorageMetadata, srcFileDir, targetFileDir string) error {
	if md.GetFileMetadata() == nil {
		return nil
	}
	f := md.GetFileMetadata()
	originalFp := fs.FilePath(srcFileDir, f)
	targetFp := fs.FilePath(targetFileDir, f)
	if err := disk.EnsureDirectoryExists(filepath.Dir(targetFp)); err != nil {
		return err
	}
	if err := os.Link(originalFp, targetFp); err == nil {
		return nil
	}
	// Linking failed :( Attempt to copy instead.
	log.Warningf("Linking failed, copying file %q => %q (may be slow)", originalFp, targetFp)
	rc, err := fs.FileReader(ctx, srcFileDir, f, 0, 0)
	if err != nil {
		return err
	}
	defer rc.Close()

	wc, err := disk.FileWriter(ctx, targetFp)
	if err != nil {
		return err
	}
	defer wc.Close()

	_, err = io.Copy(wc, rc)
	if err != nil {
		return err
	}
	return wc.Commit()
}

func (fs *fileStorer) FileWriter(ctx context.Context, fileDir string, fileRecord *rfpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error) {
	file, err := fs.FileKey(fileRecord)
	if err != nil {
		return nil, err
	}
	wc, err := disk.FileWriter(ctx, filepath.Join(fileDir, string(file)))
	if err != nil {
		return nil, err
	}
	return &fileChunker{
		CommittedWriteCloser: wc,
		fileName:             string(file),
	}, nil
}

func (fs *fileStorer) DeleteStoredFile(ctx context.Context, fileDir string, md *rfpb.StorageMetadata) error {
	switch {
	case md.GetFileMetadata() != nil:
		return os.Remove(fs.FilePath(fileDir, md.GetFileMetadata()))
	default:
		return nil
	}
}

func (fs *fileStorer) FileExists(ctx context.Context, fileDir string, md *rfpb.StorageMetadata) bool {
	switch {
	case md.GetFileMetadata() != nil:
		exists, err := disk.FileExists(ctx, fs.FilePath(fileDir, md.GetFileMetadata()))
		return exists && err == nil
	default:
		return true
	}
}
