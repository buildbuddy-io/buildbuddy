package hash

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"unsafe"
)

func String(input string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(input)))
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHash is the internal runtime hash function used by go. It uses hardware
// instructions, if available, to quickly compute a hash.
// N.B. The seed used by memhash changes on process start, so hash values
// are not deterministic across different runs of a process.
func MemHash(data []byte) uint64 {
	string := (*reflect.StringHeader)(unsafe.Pointer(&data))
	return uint64(memhash(unsafe.Pointer(string.Data), 0, uintptr(string.Len)))
}

// MemHashString is the internal runtime hash function used by go. It uses
// hardware instructions, if available, to quickly compute a hash.
// N.B. The seed used by memhash changes on process start, so hash values
// are not deterministic across different runs of a process.
func MemHashString(str string) uint64 {
	string := (*reflect.StringHeader)(unsafe.Pointer(&str))
	return uint64(memhash(unsafe.Pointer(string.Data), 0, uintptr(string.Len)))
}
