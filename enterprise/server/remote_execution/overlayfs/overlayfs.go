// Package overlayfs provides copy-on-write support for action workspaces using
// overlayfs.
//
// This package is Linux-only. All functions on non-Linux platforms return
// Unimplemented errors.
package overlayfs

import (
	"io/fs"
)

// Overlay represents a mounted overlayfs.
type Overlay struct {
	// MountDir is the path to the directory where the overlayfs is mounted.
	// This path is exposed to actions.
	MountDir string

	// UpperDir is the path to the directory which contains entries copied up
	// from the lowerdir due to writes, as well as opaque files (special files
	// indicating deletion).
	UpperDir string

	// LowerDir is the path to the directory containing the workspace files.
	// This is not exposed to actions.
	LowerDir string

	// WorkDir is the path to the directory containing temporary files used by
	// the overlayfs driver.
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
