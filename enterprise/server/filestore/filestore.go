// Package filestore implements io for reading bytestreams to/from pebble entries.
package filestore

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/jonboulle/clockwork"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	PartitionDirectoryPrefix = "PT"
	GroupIDPrefix            = "GR"

	// Data owned by the ANON user will be assigned to this groupID. This
	// ensures that our stored data has a uniform format, which allows
	// eviction to work correctly. This value should not ever need to
	// change, but there is little harm in changing it.
	AnonGroupID = "GR74042147050500190371"
)

// returns partitionID, groupID, isolation, remote_instance_name, hash
// callers may choose to use all or some of these elements when constructing
// a file path or file key. because these strings may be persisted to disk, this
// function should rarely change and must be kept backwards compatible.
func fileRecordSegments(r *sgpb.FileRecord) (partID string, groupID string, isolation string, remoteInstanceHash string, digestHash string, err error) {
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
	// PebbleKeyVersion representing an unspecified key version. When this is
	// the active key version, the database will select an active key version
	// to use based on existing database contents and the maximum known key
	// version.
	//
	// This is in a different const block so as not to affect the iota below.
	UnspecifiedPebbleKeyVersion PebbleKeyVersion = -1

	// UndefinedPebbleKeyVersion is the version of all keys in the database
	// that have not yet been versioned.
	UndefinedPebbleKeyVersion PebbleKeyVersion = iota - 1

	// PebbleKeyVersion1 is the first key version that includes a version in the
	// key path, to disambiguate reading old keys.
	PebbleKeyVersion1

	// PebbleKeyVersion2 is the same as PebbleKeyVersion1, plus a change that moves the
	// remote instance name hash to the end of the key rather than the
	// beginning. This allows for even sampling across the keyspace,
	// regardless of remote instance name.
	PebbleKeyVersion2

	// PebbleKeyVersion3 adds an optional encryption key ID for keys that refer to
	// encrypted data.
	PebbleKeyVersion3

	// PebbleKeyVersion4 includes digest type in the hash and remaps ANON data to a
	// fixed ANON group ID in GR{20} format.
	PebbleKeyVersion4

	// PebbleKeyVersion5 simplifies the keyspace (to simplify eviction) by encoding
	// AC keys under a synthetic digest made from their remote instance
	// name, groupID, and digest.
	PebbleKeyVersion5

	// MaxPebbleKeyVersion is always 1 more than the highest defined version, which
	// allows for tests to iterate across all versions from UndefinedPebbleKeyVersion
	// to MaxPebbleKeyVersion and check cross compatibility.
	MaxPebbleKeyVersion
)

// baseKey contains the common fields shared by both PebbleKey and RaftKey
type baseKey struct {
	partID             string
	groupID            string
	isolation          string
	remoteInstanceHash string
	hash               string
	encryptionKeyID    string
	digestFunction     repb.DigestFunction_Value

	// For versioned keys (PebbleKeyVersion5+, RaftKeyVersion1+), creating a key
	// from the above fields is a *lossy* procedure. This means it's impossible to
	// take a raw key and back out the groupID or other information. For that reason,
	// when a versioned key is parsed, the full key bytes are preserved here so that
	// the key can be re-serialized.
	fullKey []byte
}

func (bk baseKey) EncryptionKeyID() string {
	return bk.encryptionKeyID
}

func (bk baseKey) CacheType() rspb.CacheType {
	switch bk.isolation {
	case "ac":
		return rspb.CacheType_AC
	case "cas":
		return rspb.CacheType_CAS
	default:
		return rspb.CacheType_UNKNOWN_CACHE_TYPE
	}
}

func (bk baseKey) Partition() string {
	return PartitionDirectoryPrefix + bk.partID
}

func (bk baseKey) Hash() string {
	return bk.hash
}

// createSyntheticHash generates a synthetic hash from groupID,
// remoteInstanceHash and digest.
func (bk baseKey) createSyntheticHash() (string, error) {
	hashExtra := remapANONToFixedGroupID(bk.groupID)
	rih := bk.remoteInstanceHash
	if rih == "" {
		rih = "0"
	}
	hashExtra += "|" + rih + "|" + bk.hash
	h, err := digest.HashForDigestType(bk.digestFunction)
	if err != nil {
		return "", err
	}
	if _, err := h.Write([]byte(hashExtra)); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type PebbleKey struct {
	baseKey
}

func (pmk PebbleKey) String() string {
	fmk, err := pmk.Bytes(UndefinedPebbleKeyVersion)
	if err != nil {
		return err.Error()
	}
	return string(fmk)
}

func (pmk PebbleKey) LockID() string {
	if pmk.isolation == "ac" {
		fmk, err := pmk.Bytes(PebbleKeyVersion5)
		if err != nil {
			return err.Error()
		}
		return string(fmk)
	}
	return filepath.Join(pmk.isolation, pmk.hash)
}

type RaftKeyVersion int

const (
	UndefinedRaftKeyVersion RaftKeyVersion = iota

	RaftKeyVersion1

	// MaxRaftKeyVersion is always 1 more than the highest defined version, which
	// allows for tests to iterate across all versions from UndefinedRaftKeyVersion
	// to MaxRaftKeyVersion and check cross compatibility.
	MaxRaftKeyVersion
)

type RaftKey struct {
	baseKey
}

func (rmk RaftKey) String() string {
	partDir := PartitionDirectoryPrefix + rmk.partID
	filePath := filepath.Join(partDir, rmk.groupID, rmk.isolation, rmk.remoteInstanceHash, rmk.hash)
	return filePath
}

func (rmk RaftKey) Bytes(version RaftKeyVersion) ([]byte, error) {
	switch version {
	case RaftKeyVersion1:
		if len(rmk.fullKey) > 0 {
			return rmk.fullKey, nil
		}
		hashStr, err := rmk.createSyntheticHash()
		if err != nil {
			return nil, err
		}
		filePath := filepath.Join(hashStr, strconv.Itoa(int(rmk.digestFunction)), rmk.isolation, rmk.encryptionKeyID)
		partDir := PartitionDirectoryPrefix + rmk.partID
		filePath = filepath.Join(partDir, filePath, "v1")
		return []byte(filePath), nil
	default:
		return nil, status.FailedPreconditionErrorf("Unknown raft key version: %v", version)
	}
}

func (rmk *RaftKey) parseRaftVersion1(parts [][]byte) error {
	digestFunctionString := ""
	switch len(parts) {
	//"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/1/cas/v1",
	//"PTFOO/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/9/ac/v1",
	case 5:
		rmk.partID, rmk.hash, digestFunctionString, rmk.isolation = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3])
	//"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/cas/EK123/v1",
	//"PTFOO/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/1/ac/EK123/v1",
	case 6:
		rmk.partID, rmk.hash, digestFunctionString, rmk.isolation, rmk.encryptionKeyID = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4])
	default:
		return parseError(parts)
	}

	// Parse hash type string back into a digestFunction enum.
	intDigestFunction, err := strconv.Atoi(digestFunctionString)
	if err != nil || intDigestFunction == 0 {
		// It is an error for a v1 key to have a 0 digestFunction value.
		return parseError(parts)
	}
	rmk.digestFunction = repb.DigestFunction_Value(intDigestFunction)
	rmk.partID = strings.TrimPrefix(rmk.partID, PartitionDirectoryPrefix)

	slash := []byte{filepath.Separator}
	rmk.fullKey = bytes.Join(parts, slash)
	return nil
}

func (rmk *RaftKey) FromBytes(in []byte) (RaftKeyVersion, error) {
	slash := []byte{filepath.Separator}
	parts := bytes.Split(bytes.TrimPrefix(in, slash), slash)

	if len(parts) == 0 {
		return -1, status.InvalidArgumentErrorf("Unable to parse %q to raft key", in)
	}

	// Attempt to read the key version, if one is present. This allows for much
	// simpler parsing because we can restrict the set of valid parse inputs
	// instead of having to possibly parse any/all versions at once.
	versionInt := parseVersionFromParts(parts)
	version := UndefinedRaftKeyVersion
	if versionInt >= 0 {
		version = RaftKeyVersion(versionInt)
	}

	// Before version 1, all digests were assumed to be of type SHA256. So
	// default to that digestFunction here and in RaftKeyVersion1 onward, it will
	// be overwritten during parsing.
	rmk.digestFunction = repb.DigestFunction_SHA256

	switch version {
	case RaftKeyVersion1:
		return RaftKeyVersion1, rmk.parseRaftVersion1(parts)
	default:
		return -1, status.InvalidArgumentErrorf("Unable to parse %q to raft key", in)
	}
}

func remapANONToFixedGroupID(groupID string) string {
	if groupID == "ANON" {
		return AnonGroupID
	}
	return groupID
}

func remapFixedToANONGroupID(groupID string) string {
	if groupID == AnonGroupID {
		return "ANON"
	}
	return groupID
}

// FixedWidthGroupID returns a group ID that is zero padded to 20 digits in
// order to make all key group IDs uniform. This is necessary to be able to
// sample uniformly across group IDs.
func FixedWidthGroupID(groupID string) string {
	// This is only true for the special "ANON" group.
	if !strings.HasPrefix(groupID, GroupIDPrefix) {
		return groupID
	}
	return fmt.Sprintf("%s%020s", GroupIDPrefix, groupID[2:])
}

// Undoes the padding added by FixedWidthGroupID to produce the "real" group
// ID.
func trimFixedWidthGroupID(groupID string) string {
	// This is only for true the special "ANON" group.
	if !strings.HasPrefix(groupID, GroupIDPrefix) {
		return groupID
	}
	return GroupIDPrefix + strings.TrimLeft(groupID[2:], "0")
}

// parseVersionFromParts attempts to read the key version from the last part
// of a split key path. Returns -1 if no version is found.
// For example, if the last part is "v1", it returns 1.
func parseVersionFromParts(parts [][]byte) int {
	if len(parts) == 0 || len(parts[0]) <= 1 {
		return -1
	}
	lastPart := parts[len(parts)-1]
	if bytes.ContainsRune(lastPart[:1], 'v') {
		if s, err := strconv.ParseUint(string(lastPart[1:]), 10, 32); err == nil {
			return int(s)
		}
	}
	return -1
}

func (pmk *PebbleKey) Bytes(version PebbleKeyVersion) ([]byte, error) {
	switch version {
	case UndefinedPebbleKeyVersion:
		filePath := filepath.Join(pmk.isolation, pmk.remoteInstanceHash, pmk.hash)
		if pmk.isolation == "ac" {
			filePath = filepath.Join(pmk.groupID, filePath)
		}
		partDir := PartitionDirectoryPrefix + pmk.partID
		filePath = filepath.Join(partDir, filePath)

		return []byte(filePath), nil
	case PebbleKeyVersion1:
		filePath := filepath.Join(pmk.isolation, pmk.remoteInstanceHash, pmk.hash)
		if pmk.isolation == "ac" {
			filePath = filepath.Join(pmk.groupID, filePath)
		}
		partDir := PartitionDirectoryPrefix + pmk.partID
		filePath = filepath.Join(partDir, filePath, "v1")
		return []byte(filePath), nil
	case PebbleKeyVersion2:
		filePath := filepath.Join(pmk.hash, pmk.isolation, pmk.remoteInstanceHash)
		if pmk.isolation == "ac" {
			filePath = filepath.Join(FixedWidthGroupID(pmk.groupID), filePath)
		}
		partDir := PartitionDirectoryPrefix + pmk.partID
		filePath = filepath.Join(partDir, filePath, "v2")
		return []byte(filePath), nil
	case PebbleKeyVersion3:
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
	case PebbleKeyVersion4:
		rih := pmk.remoteInstanceHash
		if pmk.isolation == "ac" && rih == "" {
			rih = "0"
		}
		filePath := filepath.Join(pmk.hash, strconv.Itoa(int(pmk.digestFunction)), pmk.isolation, rih, pmk.encryptionKeyID)
		if pmk.isolation == "ac" {
			filePath = filepath.Join(remapANONToFixedGroupID(FixedWidthGroupID(pmk.groupID)), filePath)
		}
		partDir := PartitionDirectoryPrefix + pmk.partID
		filePath = filepath.Join(partDir, filePath, "v4")
		return []byte(filePath), nil
	case PebbleKeyVersion5:
		if len(pmk.fullKey) > 0 {
			return pmk.fullKey, nil
		}
		hashStr := pmk.hash
		if pmk.isolation == "ac" {
			var err error
			hashStr, err = pmk.createSyntheticHash()
			if err != nil {
				return nil, err
			}
		}

		filePath := filepath.Join(hashStr, strconv.Itoa(int(pmk.digestFunction)), pmk.isolation, pmk.encryptionKeyID)
		partDir := PartitionDirectoryPrefix + pmk.partID
		filePath = filepath.Join(partDir, filePath, "v5")
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

func (pmk *PebbleKey) parseVersion4(parts [][]byte) error {
	digestFunctionString := ""

	switch len(parts) {
	// CAS artifact
	// PTfoo/abcd12345asdasdasd123123123asdasdasd/1/v4
	case 5:
		pmk.partID, pmk.hash, digestFunctionString, pmk.isolation = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3])
	// encrypted CAS artifact
	// PTfoo/abcd12345asdasdasd123123123asdasdasd/1/EK123/v4
	case 6:
		pmk.partID, pmk.hash, digestFunctionString, pmk.isolation, pmk.encryptionKeyID = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4])
	// AC artifact
	// PTfoo/GR123/abcd12345asdasdasd123123123asdasdasd/1/ac/123/v4
	case 7:
		pmk.partID, pmk.groupID, pmk.hash, digestFunctionString, pmk.isolation, pmk.remoteInstanceHash = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4]), string(parts[5])
		if pmk.remoteInstanceHash == "0" {
			pmk.remoteInstanceHash = ""
		}
	// encrypted AC artifact
	// PTfoo/GR123/abcd12345asdasdasd123123123asdasdasd/1/ac/123/EK123/v4
	case 8:
		pmk.partID, pmk.groupID, pmk.hash, digestFunctionString, pmk.isolation, pmk.remoteInstanceHash, pmk.encryptionKeyID = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4]), string(parts[5]), string(parts[6])
		if pmk.remoteInstanceHash == "0" {
			pmk.remoteInstanceHash = ""
		}
	default:
		return parseError(parts)
	}

	// Parse hash type string back into a digestFunction enum.
	intDigestFunction, err := strconv.Atoi(digestFunctionString)
	if err != nil || intDigestFunction == 0 {
		// It is an error for a v4 key to have a 0 digestFunction value.
		return parseError(parts)
	}
	pmk.digestFunction = repb.DigestFunction_Value(intDigestFunction)

	pmk.partID = strings.TrimPrefix(pmk.partID, PartitionDirectoryPrefix)
	pmk.groupID = remapFixedToANONGroupID(trimFixedWidthGroupID(pmk.groupID))
	return nil
}

func (pmk *PebbleKey) parseVersion5(parts [][]byte) error {
	digestFunctionString := ""
	switch len(parts) {
	//"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/1/cas/v5",
	//"PTFOO/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/9/ac/v5",
	case 5:
		pmk.partID, pmk.hash, digestFunctionString, pmk.isolation = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3])
	//"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/cas/EK123/v5",
	//"PTFOO/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/1/ac/EK123/v5",
	case 6:
		pmk.partID, pmk.hash, digestFunctionString, pmk.isolation, pmk.encryptionKeyID = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[3]), string(parts[4])
	default:
		return parseError(parts)
	}

	// Parse hash type string back into a digestFunction enum.
	intDigestFunction, err := strconv.Atoi(digestFunctionString)
	if err != nil || intDigestFunction == 0 {
		// It is an error for a v5 key to have a 0 digestFunction value.
		return parseError(parts)
	}
	pmk.digestFunction = repb.DigestFunction_Value(intDigestFunction)
	pmk.partID = strings.TrimPrefix(pmk.partID, PartitionDirectoryPrefix)

	slash := []byte{filepath.Separator}
	pmk.fullKey = bytes.Join(parts, slash)
	return nil
}

func (pmk *PebbleKey) FromBytes(in []byte) (PebbleKeyVersion, error) {
	slash := []byte{filepath.Separator}
	parts := bytes.Split(bytes.TrimPrefix(in, slash), slash)

	if len(parts) == 0 {
		return -1, status.InvalidArgumentErrorf("Unable to parse %q to pebble key", in)
	}

	// Attempt to read the key version, if one is present. This allows for much
	// simpler parsing because we can restrict the set of valid parse inputs
	// instead of having to possibly parse any/all versions at once.
	versionInt := parseVersionFromParts(parts)
	version := UndefinedPebbleKeyVersion
	if versionInt >= 0 {
		version = PebbleKeyVersion(versionInt)
	}

	// Before version 4, all digests were assumed to be of type SHA256. So
	// default to that digestFunction here and in PebbleKeyVersion4 onward, it will
	// be overwritten during parsing.
	pmk.digestFunction = repb.DigestFunction_SHA256

	switch version {
	case UndefinedPebbleKeyVersion:
		return UndefinedPebbleKeyVersion, pmk.parseUndefinedVersion(parts)
	case PebbleKeyVersion1:
		return PebbleKeyVersion1, pmk.parseVersion1(parts)
	case PebbleKeyVersion2:
		return PebbleKeyVersion2, pmk.parseVersion2(parts)
	case PebbleKeyVersion3:
		return PebbleKeyVersion3, pmk.parseVersion3(parts)
	case PebbleKeyVersion4:
		return PebbleKeyVersion4, pmk.parseVersion4(parts)
	case PebbleKeyVersion5:
		return PebbleKeyVersion5, pmk.parseVersion5(parts)
	default:
		return -1, status.InvalidArgumentErrorf("Unable to parse %q to pebble key", in)
	}
}

type Store interface {
	FilePath(fileDir string, f *sgpb.StorageMetadata_FileMetadata) string
	PebbleKey(r *sgpb.FileRecord) (PebbleKey, error)
	RaftKey(r *sgpb.FileRecord) (RaftKey, error)

	NewReader(ctx context.Context, fileDir string, md *sgpb.StorageMetadata, offset, limit int64) (io.ReadCloser, error)

	InlineReader(f *sgpb.StorageMetadata_InlineMetadata, offset, limit int64) (io.ReadCloser, error)
	InlineWriter(ctx context.Context, sizeBytes int64) interfaces.MetadataWriteCloser

	FileReader(ctx context.Context, fileDir string, f *sgpb.StorageMetadata_FileMetadata, offset, limit int64) (io.ReadCloser, error)
	FileWriter(ctx context.Context, fileDir string, fileRecord *sgpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error)

	BlobReader(ctx context.Context, b *sgpb.StorageMetadata_GCSMetadata, offset, limit int64) (io.ReadCloser, error)
	BlobWriter(ctx context.Context, fileRecord *sgpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error)
	DeleteStoredBlob(ctx context.Context, b *sgpb.StorageMetadata_GCSMetadata) error
	UpdateBlobAtime(ctx context.Context, b *sgpb.StorageMetadata_GCSMetadata, t time.Time) error

	DeleteStoredFile(ctx context.Context, fileDir string, md *sgpb.StorageMetadata) error
	FileExists(ctx context.Context, fileDir string, md *sgpb.StorageMetadata) bool
}

type PebbleGCSStorage interface {
	SetBucketCustomTimeTTL(ctx context.Context, ageInDays int64) error
	Reader(ctx context.Context, blobName string, offset, limit int64) (io.ReadCloser, error)
	ConditionalWriter(ctx context.Context, blobName string, overwriteExisting bool, customTime time.Time, estimatedSize int64) (interfaces.CommittedWriteCloser, error)
	DeleteBlob(ctx context.Context, blobName string) error
	UpdateCustomTime(ctx context.Context, blobName string, t time.Time) error
}

type Options struct {
	gcs     PebbleGCSStorage
	appName string
	clock   clockwork.Clock
}

type Option func(*Options)

func WithGCSBlobstore(gcs PebbleGCSStorage, appName string) Option {
	return func(o *Options) {
		o.gcs = gcs
		o.appName = appName
	}
}

func WithClock(c clockwork.Clock) Option {
	return func(o *Options) {
		o.clock = c
	}
}

type fileStorer struct {
	gcs     PebbleGCSStorage
	appName string
	clock   clockwork.Clock
}

// New creates a new filestorer interface.
func New(opts ...Option) Store {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	if options.clock == nil {
		options.clock = clockwork.NewRealClock()
	}
	return &fileStorer{
		gcs:     options.gcs,
		appName: options.appName,
		clock:   options.clock,
	}
}

func (fs *fileStorer) FilePath(fileDir string, f *sgpb.StorageMetadata_FileMetadata) string {
	fp := f.GetFilename()
	if !filepath.IsAbs(fp) {
		fp = filepath.Join(fileDir, f.GetFilename())
	}
	return fp
}

// fileKey is the partial path where a file will be written.
// For example, given a fileRecord with fileKey: "foo/bar", the filestore will
// write the file at a path like "/root/dir/blobs/foo/bar".
func (fs *fileStorer) fileKey(r *sgpb.FileRecord) ([]byte, error) {
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

// blobKey is the partial path where a blob will be written.
// For example, given a fileRecord with FileKey: "foo/bar", the filestore will
// write the file at a path like "/buildbuddy-app-1/blobs/foo/bar".
func blobKey(appName string, r *sgpb.FileRecord) ([]byte, error) {
	// This function cannot change without a data migration.
	// blobkeys look like this:
	//   // {appName}/{partitionID}/{groupID}/{ac|cas}/{hashPrefix:4}/{hash}
	//   // for example:
	//   //   buildbuddy-app-0/PART123/GR123/ac/44321/abcd/abcd12345asdasdasd123123123asdasdasd
	//   //   buildbuddy-app-0/PART123/GR124/cas/abcd/abcd12345asdasdasd123123123asdasdasd
	if appName == "" {
		return nil, status.FailedPreconditionErrorf("Invalid app name: %q (cannot be empty)", appName)
	}
	partID, groupID, isolation, remoteInstanceHash, hash, err := fileRecordSegments(r)
	if err != nil {
		return nil, err
	}
	if r.GetEncryption().GetKeyId() != "" {
		hash += "_" + r.GetEncryption().GetKeyId()
	}
	partDir := PartitionDirectoryPrefix + partID
	if r.GetIsolation().GetCacheType() == rspb.CacheType_AC {
		return []byte(filepath.Join(appName, partDir, groupID, isolation, remoteInstanceHash, hash[:4], hash)), nil
	} else {
		return []byte(filepath.Join(appName, partDir, isolation, remoteInstanceHash, hash[:4], hash)), nil
	}
}

func (fs *fileStorer) PebbleKey(r *sgpb.FileRecord) (PebbleKey, error) {
	if r.GetDigestFunction() == repb.DigestFunction_UNKNOWN {
		return PebbleKey{}, status.FailedPreconditionError("FileRecord did not have a digestFunction set")
	}

	partID, groupID, isolation, remoteInstanceHash, hash, err := fileRecordSegments(r)
	if err != nil {
		return PebbleKey{}, err
	}
	return PebbleKey{
		baseKey: baseKey{
			partID:             partID,
			groupID:            groupID,
			isolation:          isolation,
			remoteInstanceHash: remoteInstanceHash,
			hash:               hash,
			encryptionKeyID:    r.GetEncryption().GetKeyId(),
			digestFunction:     r.GetDigestFunction(),
		},
	}, nil
}

func (fs *fileStorer) RaftKey(r *sgpb.FileRecord) (RaftKey, error) {
	partID, groupID, isolation, remoteInstanceHash, hash, err := fileRecordSegments(r)
	if err != nil {
		return RaftKey{}, err
	}
	return RaftKey{
		baseKey: baseKey{
			partID:             partID,
			groupID:            groupID,
			isolation:          isolation,
			remoteInstanceHash: remoteInstanceHash,
			hash:               hash,
			encryptionKeyID:    r.GetEncryption().GetKeyId(),
			digestFunction:     r.GetDigestFunction(),
		},
	}, nil
}

func (fs *fileStorer) NewReader(ctx context.Context, fileDir string, md *sgpb.StorageMetadata, offset, limit int64) (io.ReadCloser, error) {
	switch {
	case md.GetFileMetadata() != nil:
		return fs.FileReader(ctx, fileDir, md.GetFileMetadata(), offset, limit)
	case md.GetInlineMetadata() != nil:
		return fs.InlineReader(md.GetInlineMetadata(), offset, limit)
	case md.GetGcsMetadata() != nil:
		return fs.BlobReader(ctx, md.GetGcsMetadata(), offset, limit)
	default:
		return nil, status.InvalidArgumentErrorf("No stored metadata: %+v", md)
	}
}

func (fs *fileStorer) InlineReader(f *sgpb.StorageMetadata_InlineMetadata, offset, limit int64) (io.ReadCloser, error) {
	data := f.GetData()[offset:]
	if limit > 0 && limit < int64(len(data)) {
		data = data[:limit]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type inlineWriter struct {
	*bytes.Buffer
	writtenAt time.Time
}

func (iw *inlineWriter) Close() error {
	return nil
}

func (iw *inlineWriter) Metadata() *sgpb.StorageMetadata {
	return &sgpb.StorageMetadata{
		InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
			Data:          iw.Buffer.Bytes(),
			CreatedAtNsec: iw.writtenAt.UnixNano(),
		},
	}
}

func (fs *fileStorer) InlineWriter(ctx context.Context, sizeBytes int64) interfaces.MetadataWriteCloser {
	return &inlineWriter{
		Buffer:    bytes.NewBuffer(make([]byte, 0, sizeBytes)),
		writtenAt: fs.clock.Now(),
	}
}

type fileChunker struct {
	interfaces.CommittedWriteCloser
	fileName string
}

func (c *fileChunker) Metadata() *sgpb.StorageMetadata {
	return &sgpb.StorageMetadata{
		FileMetadata: &sgpb.StorageMetadata_FileMetadata{
			Filename: c.fileName,
		},
	}
}

func (fs *fileStorer) FileReader(ctx context.Context, fileDir string, f *sgpb.StorageMetadata_FileMetadata, offset, limit int64) (io.ReadCloser, error) {
	fp := fs.FilePath(fileDir, f)
	return disk.FileReader(ctx, fp, offset, limit)
}

func (fs *fileStorer) FileWriter(ctx context.Context, fileDir string, fileRecord *sgpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error) {
	file, err := fs.fileKey(fileRecord)
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

func (fs *fileStorer) BlobReader(ctx context.Context, b *sgpb.StorageMetadata_GCSMetadata, offset, limit int64) (io.ReadCloser, error) {
	if fs.gcs == nil || fs.appName == "" {
		return nil, status.FailedPreconditionError("gcs blobstore or appName not configured")
	}
	return fs.gcs.Reader(ctx, b.GetBlobName(), offset, limit)
}

type gcsMetadataWriter struct {
	interfaces.CommittedWriteCloser
	ctx        context.Context
	blobName   string
	customTime time.Time
}

func (g *gcsMetadataWriter) Commit() error {
	_, spn := tracing.StartSpan(g.ctx)
	defer spn.End()
	err := g.CommittedWriteCloser.Commit()

	switch {
	case status.IsAlreadyExistsError(err):
		log.Debugf("Write gcs blob %q (already exists)", g.blobName)
		return nil
	case status.IsResourceExhaustedError(err):
		// gcs.ConditionalWriter returns this when there are too many writes to
		// the same object. We can assume that another write was successful.
		log.Debugf("Write gcs blob %q (too many writes)", g.blobName)
		return nil
	default:
		return err
	}
}

func (g *gcsMetadataWriter) Close() error {
	// We're simply cancelling a context in Close() so we don't care about
	// the value of the returned error. It will have already been returned
	// and checked in Commit().
	_ = g.CommittedWriteCloser.Close()
	return nil
}

func (g *gcsMetadataWriter) Metadata() *sgpb.StorageMetadata {
	return &sgpb.StorageMetadata{
		GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{
			BlobName:           g.blobName,
			LastCustomTimeUsec: g.customTime.UnixMicro(),
		},
	}
}

func (fs *fileStorer) BlobWriter(ctx context.Context, fileRecord *sgpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error) {
	if fs.gcs == nil || fs.appName == "" {
		return nil, status.FailedPreconditionError("gcs blobstore or appName not configured")
	}
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	blobNameBytes, err := blobKey(fs.appName, fileRecord)
	if err != nil {
		return nil, err
	}
	salt, err := random.RandomString(5)
	if err != nil {
		return nil, err
	}
	blobName := string(blobNameBytes) + "-" + salt

	estimatedSize := fileRecord.GetDigest().GetSizeBytes()
	if fileRecord.GetCompressor() != repb.Compressor_IDENTITY {
		// Guess that the compressed data will be 1/5th the size. The estimated
		// size triggers an optimization for very large files, so it's better to
		// underestimate.
		estimatedSize /= 5
	}
	customTime := fs.clock.Now()
	wc, err := fs.gcs.ConditionalWriter(ctx, blobName, true /*=overwriteExisting*/, customTime, estimatedSize)
	if err != nil {
		return nil, err
	}
	return &gcsMetadataWriter{
		ctx:                  ctx,
		CommittedWriteCloser: wc,
		blobName:             string(blobName),
		customTime:           customTime,
	}, nil
}

func (fs *fileStorer) DeleteStoredBlob(ctx context.Context, b *sgpb.StorageMetadata_GCSMetadata) error {
	if fs.gcs == nil || fs.appName == "" {
		return status.FailedPreconditionError("gcs blobstore or appName not configured")
	}
	err := fs.gcs.DeleteBlob(ctx, b.GetBlobName())
	log.Debugf("Deleted gcs blob: %q with err: %s", b.GetBlobName(), err)
	return err
}

func (fs *fileStorer) UpdateBlobAtime(ctx context.Context, b *sgpb.StorageMetadata_GCSMetadata, t time.Time) error {
	if fs.gcs == nil || fs.appName == "" {
		return status.FailedPreconditionError("gcs blobstore or appName not configured")
	}
	err := fs.gcs.UpdateCustomTime(ctx, b.GetBlobName(), t)
	log.Debugf("Updated gcs blob: %q atime to %d with err: %s", b.GetBlobName(), t.UnixMicro(), err)
	return err
}

func (fs *fileStorer) DeleteStoredFile(ctx context.Context, fileDir string, md *sgpb.StorageMetadata) error {
	switch {
	case md.GetFileMetadata() != nil:
		return os.Remove(fs.FilePath(fileDir, md.GetFileMetadata()))
	default:
		return nil
	}
}

func (fs *fileStorer) FileExists(ctx context.Context, fileDir string, md *sgpb.StorageMetadata) bool {
	switch {
	case md.GetFileMetadata() != nil:
		exists, err := disk.FileExists(ctx, fs.FilePath(fileDir, md.GetFileMetadata()))
		return exists && err == nil
	default:
		return true
	}
}
