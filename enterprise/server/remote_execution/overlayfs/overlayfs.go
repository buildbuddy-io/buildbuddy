package overlayfs

import (
	"io/fs"
)

// Overlay represents a mounted overlayfs.
type Overlay struct {
	// MountDir contains the actual overlayfs mount. This is the path exposed
	// to actions.
	MountDir string
	// UpperDir contains files that were copied up from the lowerdir due to
	// writes, as well as opaque files (special files indicating deletion).
	UpperDir string
	// LowerDir contains the workspace files which cannot be modified by
	// actions.
	LowerDir string
	// WorkDir contains temporary files used by the overlayfs driver.
	WorkDir string

	opts Opts
}

type Opts struct {
	// DirPerms are the permissions to use when creating directories.
	DirPerms fs.FileMode
}

type ApplyOpts struct {
	// AllowRename determines whether files are allowed to be re-linked from the
	// upper dir to the lower dir when applying the upperdir. This avoids the
	// performance overhead of the default copy behavior, but must only be used
	// in cases where it is certain that the guest workload does not have any
	// open file handles in the overlay workspace. Specifically, when runner
	// recycling is enabled, it's generally unsafe to enable this option, since
	// some persistent processes may be left running from the workload.
	AllowRename bool
}
