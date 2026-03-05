package vfscommon

// RootInodeId is the ID of the root directory.
const RootInodeId = 1

// InodeEntryInvalidation represents a named entry within a directory inode
// to be invalidated.
type InodeEntryInvalidation struct {
	// InodeID is the inode id of the parent directory.
	InodeID uint64
	// Name is the child that needs to be invalidated.
	Name string
}

// InodeInvalidations represents a set of invalidations that need to be applied
// to the virtual filesystem.
//
// Invalidations need only be performed for filesystem changes that occur w/o
// going through the kernel.
type InodeInvalidations struct {
	// Content is a slice of inode IDs whose content needs to be invalidated.
	// Scenarios when content needs to be invalidated:
	//  - If the contents of a directory has changed, then the directory inode
	//    needs to be invalidated.
	//  - If the data contents of a file has changed, then the file inode
	//    needs to be invalidated.
	Content []uint64
	// Entry is a slice of entry invalidations to be performed.
	Entry []InodeEntryInvalidation
}
