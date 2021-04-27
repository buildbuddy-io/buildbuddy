package id

import (
	//MD5 is fine here, it's just making an ID and is not a security application
	"crypto/md5" // #nosec G501
	"encoding/binary"
)

func InvocationPKFromID(iid string) int64 {
	hash := md5.Sum([]byte(iid)) // #nosec G401
	return int64(binary.BigEndian.Uint64(hash[:8]))
}

func TargetIDFromTargetPath(path string) int64 {
	hash := md5.Sum([]byte(path)) // #nosec G401
	return int64(binary.BigEndian.Uint64(hash[:8]))
}
