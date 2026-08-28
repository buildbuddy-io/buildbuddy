package ext4writer

// Extended attributes: stored in the inode's extra space when they fit,
// otherwise in one external xattr block per inode (ext_attr feature).
// Container images need these (e.g. security.capability on binaries).

import (
	"encoding/binary"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
)

const (
	xattrMagic          = 0xEA020000
	featureCompatExtAttr = 0x0008
	// Space available for in-inode xattrs: inode size minus the 128-byte
	// base and the 32-byte extra fields, minus the 4-byte magic.
	inInodeXattrBytes = inodeSize - 128 - 32 - 4
)

type xattr struct {
	index uint8 // ext4 name index
	name  string
	value []byte
}

// xattrIndex splits a full attribute name into an ext4 name index and the
// remaining name.
func xattrIndex(full string) (uint8, string, bool) {
	switch {
	case strings.HasPrefix(full, "user."):
		return 1, full[5:], true
	case full == "system.posix_acl_access":
		return 2, "", true
	case full == "system.posix_acl_default":
		return 3, "", true
	case strings.HasPrefix(full, "trusted."):
		return 4, full[8:], true
	case strings.HasPrefix(full, "security."):
		return 6, full[9:], true
	case strings.HasPrefix(full, "system."):
		return 7, full[7:], true
	}
	return 0, "", false
}

// readXattrs lists and reads a path's extended attributes (not following
// symlinks).
func readXattrs(path string) ([]xattr, error) {
	names := make([]byte, 4096)
	n, err := unix.Llistxattr(path, names)
	if err != nil {
		if err == unix.ENOTSUP || err == unix.EOPNOTSUPP || err == unix.ENODATA {
			return nil, nil
		}
		if err == unix.ERANGE {
			sz, err := unix.Llistxattr(path, nil)
			if err != nil {
				return nil, err
			}
			names = make([]byte, sz)
			if n, err = unix.Llistxattr(path, names); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	var out []xattr
	for full := range strings.SplitSeq(strings.TrimRight(string(names[:n]), "\x00"), "\x00") {
		if full == "" {
			continue
		}
		idx, name, ok := xattrIndex(full)
		if !ok {
			continue // unknown namespace; ext4 couldn't store it either
		}
		sz, err := unix.Lgetxattr(path, full, nil)
		if err != nil {
			return nil, err
		}
		val := make([]byte, sz)
		if sz > 0 {
			if _, err := unix.Lgetxattr(path, full, val); err != nil {
				return nil, err
			}
		}
		out = append(out, xattr{index: idx, name: name, value: val})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.index != b.index {
			return a.index < b.index
		}
		if len(a.name) != len(b.name) {
			return len(a.name) < len(b.name)
		}
		return a.name < b.name
	})
	return out, nil
}

func xattrEntryLen(name string) int { return (16 + len(name) + 3) &^ 3 }

func xattrValueLen(v []byte) int { return (len(v) + 3) &^ 3 }

// xattrHashEntry computes e_hash as the kernel does.
func xattrHashEntry(name string, value []byte) uint32 {
	var h uint32
	for i := 0; i < len(name); i++ {
		h = (h << 5) ^ (h >> 27) ^ uint32(name[i])
	}
	for i := 0; i < len(value); i += 4 {
		var w [4]byte
		copy(w[:], value[i:])
		h = (h << 16) ^ (h >> 16) ^ binary.LittleEndian.Uint32(w[:])
	}
	return h
}

// xattrsFitInInode reports whether the attributes fit in the inode body.
func xattrsFitInInode(xs []xattr) bool {
	need := 4 // terminating null entry
	for _, x := range xs {
		need += xattrEntryLen(x.name) + xattrValueLen(x.value)
	}
	return need <= inInodeXattrBytes
}

// encodeXattrs serializes entries + values into a region of the given size.
// valueBase is the offset that e_value_offs is relative to, measured from
// the start of the entries (0 for in-inode, where offsets are relative to
// the first entry; for a block, entries start at 32 and offsets are relative
// to the block start, so valueBase = 32). hashed selects whether e_hash is
// filled in: the kernel requires it for external blocks and requires zero for
// in-inode entries (it doesn't maintain those).
func encodeXattrs(xs []xattr, region []byte, valueBase int, hashed bool) {
	off := 0
	valEnd := len(region)
	for _, x := range xs {
		vl := xattrValueLen(x.value)
		valEnd -= vl
		e := region[off:]
		e[0] = uint8(len(x.name))
		e[1] = x.index
		binary.LittleEndian.PutUint16(e[2:], uint16(valEnd+valueBase))
		binary.LittleEndian.PutUint32(e[4:], 0) // e_value_inum
		binary.LittleEndian.PutUint32(e[8:], uint32(len(x.value)))
		if hashed {
			binary.LittleEndian.PutUint32(e[12:], xattrHashEntry(x.name, x.value))
		}
		copy(e[16:], x.name)
		copy(region[valEnd:], x.value)
		off += xattrEntryLen(x.name)
	}
	// Null terminator entry (already zero).
}

// encodeInodeXattrs writes the in-inode xattr region of a 256-byte inode.
func encodeInodeXattrs(b []byte, xs []xattr) {
	region := b[128+32:]
	binary.LittleEndian.PutUint32(region, xattrMagic)
	encodeXattrs(xs, region[4:], 0, false)
}

// encodeXattrBlock renders an external xattr block.
func encodeXattrBlock(xs []xattr) ([]byte, error) {
	need := 32 + 4
	for _, x := range xs {
		need += xattrEntryLen(x.name) + xattrValueLen(x.value)
	}
	if need > blockSize {
		return nil, status.InvalidArgumentErrorf("extended attributes do not fit in one block (%d bytes)", need)
	}
	b := make([]byte, blockSize)
	le := binary.LittleEndian
	le.PutUint32(b[0:], xattrMagic)
	le.PutUint32(b[4:], 1) // h_refcount
	le.PutUint32(b[8:], 1) // h_blocks
	encodeXattrs(xs, b[32:], 32, true)
	// h_hash: combination of entry hashes.
	var h uint32
	for _, x := range xs {
		eh := xattrHashEntry(x.name, x.value)
		if eh == 0 {
			h = 0
			break
		}
		h = (h << 16) ^ (h >> 16) ^ eh
	}
	le.PutUint32(b[12:], h)
	return b, nil
}
