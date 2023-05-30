// Package filestore implements io for reading bytestreams to/from pebble entries.
package filestore

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	PartitionDirectoryPrefix = "PT"
	groupIDPrefix            = "GR"
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

	if r.GetIsolation().GetCacheType() == rspb.CacheType_CAS {
		isolation = "cas"
	} else if r.GetIsolation().GetCacheType() == rspb.CacheType_AC {
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

type PebbleKeyVersion int

const (
	// UndefinedKeyVersion is the version of all keys in the database
	// that have not yet been versioned.
	UndefinedKeyVersion PebbleKeyVersion = iota

	// Version1 is the first key version that includes a version in the
	// key path, to disambiguate reading old keys.
	Version1

	// Version2 is the same as Version1, plus a change that moves the
	// remote instance name hash to the end of the key rather than the
	// beginning. This allows for even sampling across the keyspace,
	// regardless of remote instance name.
	Version2

	// Version3 adds an optional encryption key ID for keys that refer to
	// encrypted data.
	Version3

	// TestingMaxKeyVersion should not be used directly -- it is always
	// 1 more than the highest defined version, which allows for tests
	// to iterate across all versions from UndefinedKeyVersion to
	// TestingMaxKeyVersion and check cross compatibility.
	TestingMaxKeyVersion
)

type PebbleKey struct {
	partID             string
	groupID            string
	isolation          string
	remoteInstanceHash string
	hash               string
	encryptionKeyID    string
}

func (pmk PebbleKey) String() string {
	fmk, err := pmk.Bytes(UndefinedKeyVersion)
	if err != nil {
		return err.Error()
	}
	return string(fmk)
}

func (pmk PebbleKey) LockID() string {
	if pmk.isolation == "ac" {
		return filepath.Join(pmk.isolation, pmk.groupID, pmk.remoteInstanceHash, pmk.hash)
	}
	return filepath.Join(pmk.isolation, pmk.hash)
}

func (pmk PebbleKey) CacheType() rspb.CacheType {
	switch pmk.isolation {
	case "ac":
		return rspb.CacheType_AC
	case "cas":
		return rspb.CacheType_CAS
	default:
		return rspb.CacheType_UNKNOWN_CACHE_TYPE
	}
}

func (pmk PebbleKey) Hash() string {
	return pmk.hash
}

func (pmk PebbleKey) GroupID() string {
	return pmk.groupID
}

// FixedWidthGroupID returns a group ID that is zero padded to 20 digits in
// order to make all key group IDs uniform. This is necessary to be able to
// sample uniformly across group IDs.
func FixedWidthGroupID(groupID string) string {
	// This is only for true the special "ANON" group.
	if !strings.HasPrefix(groupID, groupIDPrefix) {
		return groupID
	}
	return fmt.Sprintf("%s%020s", groupIDPrefix, groupID[2:])
}

// Undoes the padding added by FixedWidthGroupID to produce the "real" group
// ID.
func trimFixedWidthGroupID(groupID string) string {
	// This is only for true the special "ANON" group.
	if !strings.HasPrefix(groupID, groupIDPrefix) {
		return groupID
	}
	return groupIDPrefix + strings.TrimLeft(groupID[2:], "0")
}

func (pmk *PebbleKey) Bytes(version PebbleKeyVersion) ([]byte, error) {
	switch version {
	case UndefinedKeyVersion:
		filePath := filepath.Join(pmk.isolation, pmk.remoteInstanceHash, pmk.hash)
		if pmk.isolation == "ac" {
			filePath = filepath.Join(pmk.groupID, filePath)
		}
		partDir := PartitionDirectoryPrefix + pmk.partID
		filePath = filepath.Join(partDir, filePath)

		return []byte(filePath), nil
	case Version1:
		filePath := filepath.Join(pmk.isolation, pmk.remoteInstanceHash, pmk.hash)
		if pmk.isolation == "ac" {
			filePath = filepath.Join(pmk.groupID, filePath)
		}
		partDir := PartitionDirectoryPrefix + pmk.partID
		filePath = filepath.Join(partDir, filePath, "v1")
		return []byte(filePath), nil
	case Version2:
		filePath := filepath.Join(pmk.hash, pmk.isolation, pmk.remoteInstanceHash)
		if pmk.isolation == "ac" {
			filePath = filepath.Join(FixedWidthGroupID(pmk.groupID), filePath)
		}
		partDir := PartitionDirectoryPrefix + pmk.partID
		filePath = filepath.Join(partDir, filePath, "v2")
		return []byte(filePath), nil
	case Version3:
		rih := pmk.remoteInstanceHash
		if pmk.isolation == "ac" && rih == "" {
			rih = "0"
		}
		filePath := filepath.Join(pmk.hash, pmk.isolation, rih, pmk.encryptionKeyID)
		if pmk.isolation == "ac" {
			filePath = filepath.Join(FixedWidthGroupID(pmk.groupID), filePath)
		}
		partDir := PartitionDirectoryPrefix + pmk.partID
		filePath = filepath.Join(partDir, filePath, "v3")
		return []byte(filePath), nil
	default:
		return nil, status.FailedPreconditionErrorf("Unknown key version: %v", version)
	}
}

func parseError(parts [][]byte) error {
	return status.InvalidArgumentErrorf("Unable to parse %v to pebble key", string(bytes.Join(parts, []byte("/"))))
}

func (pmk *PebbleKey) parseUndefinedVersion(parts [][]byte) error {
	switch len(parts) {
	case 3:
		pmk.partID, pmk.isolation, pmk.hash = string(parts[0]), string(parts[1]), string(parts[2])
	case 4:
		pmk.partID, pmk.groupID, pmk.isolation, pmk.hash = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3])
	case 5:
		pmk.partID, pmk.groupID, pmk.isolation, pmk.remoteInstanceHash, pmk.hash = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4])
	default:
		return parseError(parts)
	}
	pmk.partID = strings.TrimPrefix(pmk.partID, PartitionDirectoryPrefix)
	return nil
}

func (pmk *PebbleKey) parseVersion1(parts [][]byte) error {
	switch len(parts) {
	case 4:
		pmk.partID, pmk.isolation, pmk.hash = string(parts[0]), string(parts[1]), string(parts[2])
	case 5:
		pmk.partID, pmk.groupID, pmk.isolation, pmk.hash = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3])
	case 6:
		pmk.partID, pmk.groupID, pmk.isolation, pmk.remoteInstanceHash, pmk.hash = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4])
	default:
		return parseError(parts)
	}
	pmk.partID = strings.TrimPrefix(pmk.partID, PartitionDirectoryPrefix)
	return nil
}

func (pmk *PebbleKey) parseVersion2(parts [][]byte) error {
	switch len(parts) {
	case 4:
		pmk.partID, pmk.hash, pmk.isolation = string(parts[0]), string(parts[1]), string(parts[2])
	case 5:
		pmk.partID, pmk.groupID, pmk.hash, pmk.isolation = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3])
	case 6:
		pmk.partID, pmk.groupID, pmk.hash, pmk.isolation, pmk.remoteInstanceHash = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4])
	default:
		return parseError(parts)
	}
	pmk.partID = strings.TrimPrefix(pmk.partID, PartitionDirectoryPrefix)
	pmk.groupID = trimFixedWidthGroupID(pmk.groupID)
	return nil
}

func (pmk *PebbleKey) parseVersion3(parts [][]byte) error {
	switch len(parts) {
	// CAS artifact
	// PTfoo/abcd12345asdasdasd123123123asdasdasd/v3
	case 4:
		pmk.partID, pmk.hash, pmk.isolation = string(parts[0]), string(parts[1]), string(parts[2])
	// encrypted CAS artifact
	// PTfoo/abcd12345asdasdasd123123123asdasdasd/EK123/v3
	case 5:
		pmk.partID, pmk.hash, pmk.isolation, pmk.encryptionKeyID = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3])
	// AC artifact
	// PTfoo/GR123/abcd12345asdasdasd123123123asdasdasd/ac/123/v3
	case 6:
		pmk.partID, pmk.groupID, pmk.hash, pmk.isolation, pmk.remoteInstanceHash = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4])
		if pmk.remoteInstanceHash == "0" {
			pmk.remoteInstanceHash = ""
		}
	// encrypted AC artifact
	// PTfoo/GR123/abcd12345asdasdasd123123123asdasdasd/ac/123/EK123/v3
	case 7:
		pmk.partID, pmk.groupID, pmk.hash, pmk.isolation, pmk.remoteInstanceHash, pmk.encryptionKeyID = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4]), string(parts[5])
		if pmk.remoteInstanceHash == "0" {
			pmk.remoteInstanceHash = ""
		}
	default:
		return parseError(parts)
	}
	pmk.partID = strings.TrimPrefix(pmk.partID, PartitionDirectoryPrefix)
	pmk.groupID = trimFixedWidthGroupID(pmk.groupID)
	return nil
}

func (pmk *PebbleKey) FromBytes(in []byte) (PebbleKeyVersion, error) {
	version := UndefinedKeyVersion
	slash := []byte{filepath.Separator}
	parts := bytes.Split(bytes.TrimPrefix(in, slash), slash)

	if len(parts) == 0 {
		return -1, status.InvalidArgumentErrorf("Unable to parse %q to pebble key", in)
	}

	// Attempt to read the key version, if one is present. This allows for much
	// simpler parsing because we can restrict the set of valid parse inputs
	// instead of having to possibly parse any/all versions at once.
	if len(parts[0]) > 1 {
		lastPart := parts[len(parts)-1]
		if bytes.ContainsRune(lastPart[:1], 'v') {
			if s, err := strconv.ParseUint(string(lastPart[1:]), 10, 32); err == nil {
				version = PebbleKeyVersion(s)
			}
		}
	}

	switch version {
	case UndefinedKeyVersion:
		return UndefinedKeyVersion, pmk.parseUndefinedVersion(parts)
	case Version1:
		return Version1, pmk.parseVersion1(parts)
	case Version2:
		return Version2, pmk.parseVersion2(parts)
	case Version3:
		return Version3, pmk.parseVersion3(parts)
	default:
		return -1, status.InvalidArgumentErrorf("Unable to parse %q to pebble key", in)
	}
}

type Store interface {
	FileKey(r *rfpb.FileRecord) ([]byte, error)
	FilePath(fileDir string, f *rfpb.StorageMetadata_FileMetadata) string
	FileMetadataKey(r *rfpb.FileRecord) ([]byte, error)
	PebbleKey(r *rfpb.FileRecord) (PebbleKey, error)

	NewReader(ctx context.Context, fileDir string, md *rfpb.StorageMetadata, offset, limit int64) (io.ReadCloser, error)
	NewWriter(ctx context.Context, fileDir string, fileRecord *rfpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error)

	InlineReader(f *rfpb.StorageMetadata_InlineMetadata, offset, limit int64) (io.ReadCloser, error)
	InlineWriter(ctx context.Context, sizeBytes int64) interfaces.MetadataWriteCloser

	FileReader(ctx context.Context, fileDir string, f *rfpb.StorageMetadata_FileMetadata, offset, limit int64) (io.ReadCloser, error)
	FileWriter(ctx context.Context, fileDir string, fileRecord *rfpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error)

	DeleteStoredFile(ctx context.Context, fileDir string, md *rfpb.StorageMetadata) error
	FileExists(ctx context.Context, fileDir string, md *rfpb.StorageMetadata) bool

	LinkOrCopyFile(ctx context.Context, md *rfpb.StorageMetadata, dstFileRecord *rfpb.FileRecord, srcFileDir, targetFileDir string) (*rfpb.StorageMetadata, error)
}

type fileStorer struct {
}

// New creates a new filestorer interface.
func New() Store {
	return &fileStorer{}
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
	if r.GetEncryption().GetKeyId() != "" {
		hash += "_" + r.GetEncryption().GetKeyId()
	}
	partDir := PartitionDirectoryPrefix + partID
	if r.GetIsolation().GetCacheType() == rspb.CacheType_AC {
		return []byte(filepath.Join(partDir, groupID, isolation, remoteInstanceHash, hash[:4], hash)), nil
	} else {
		return []byte(filepath.Join(partDir, isolation, remoteInstanceHash, hash[:4], hash)), nil
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
	pmk, err := fs.PebbleKey(r)
	if err != nil {
		return nil, err
	}
	return pmk.Bytes(UndefinedKeyVersion)
}

func (fs *fileStorer) PebbleKey(r *rfpb.FileRecord) (PebbleKey, error) {
	partID, groupID, isolation, remoteInstanceHash, hash, err := fileRecordSegments(r)
	if err != nil {
		return PebbleKey{}, err
	}
	return PebbleKey{
		partID:             partID,
		groupID:            groupID,
		isolation:          isolation,
		remoteInstanceHash: remoteInstanceHash,
		hash:               hash,
		encryptionKeyID:    r.GetEncryption().GetKeyId(),
	}, nil
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

func (fs *fileStorer) LinkOrCopyFile(ctx context.Context, md *rfpb.StorageMetadata, dstFileRecord *rfpb.FileRecord, srcFileDir, targetFileDir string) (*rfpb.StorageMetadata, error) {
	if md.GetFileMetadata() == nil {
		return md, nil
	}
	f := md.GetFileMetadata()
	originalFp := fs.FilePath(srcFileDir, f)
	dstfileKey, err := fs.FileKey(dstFileRecord)
	if err != nil {
		return nil, err
	}
	targetFp := filepath.Join(targetFileDir, string(dstfileKey))
	if err := disk.EnsureDirectoryExists(filepath.Dir(targetFp)); err != nil {
		return nil, err
	}

	newMD := &rfpb.StorageMetadata{
		FileMetadata: &rfpb.StorageMetadata_FileMetadata{
			Filename: string(dstfileKey),
		},
	}
	if err := os.Link(originalFp, targetFp); err == nil {
		return newMD, nil
	}
	// Linking failed :( Attempt to copy instead.
	log.Warningf("Linking failed, copying file %q => %q (may be slow)", originalFp, targetFp)
	rc, err := fs.FileReader(ctx, srcFileDir, f, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	wc, err := disk.FileWriter(ctx, targetFp)
	if err != nil {
		return nil, err
	}
	defer wc.Close()

	_, err = io.Copy(wc, rc)
	if err != nil {
		return nil, err
	}
	return newMD, wc.Commit()
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
