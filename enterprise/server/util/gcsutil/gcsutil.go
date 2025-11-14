package gcsutil

import (
	"time"

	"github.com/jonboulle/clockwork"

	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

// ObjectIsPastTTL checks whether a GCS object has passed its TTL.
// The GCS TTL is set as an integer number of days. The docs are vague,
// but it seems plausible that if a file is *ever* marked for deletion,
// it will be deleted, even if it has changed since. Basically, there is
// one job marking files for deletion, and another job deleting them,
// and if the object has changed between those two events, it is still
// deleted.
//
// For this reason, if a GCS object is ever less than 1 hour away from
// TTL, assume it has already been marked for deletion.
func ObjectIsPastTTL(clock clockwork.Clock, gcsMetadata *sgpb.StorageMetadata_GCSMetadata, gcsTTLDays int64) bool {
	customTimeUsec := gcsMetadata.GetLastCustomTimeUsec()
	buffer := time.Hour
	return clock.Since(time.UnixMicro(customTimeUsec))+buffer > time.Duration(gcsTTLDays*24)*time.Hour
}
