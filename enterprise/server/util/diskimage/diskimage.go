// Package diskimage defines disk image formats shared by image conversion and
// snapshot code, without depending on Linux-only packages.
package diskimage

// Format is the filesystem format of a converted container image. Its value
// is the filesystem's type name for mount(2).
type Format string

const (
	Ext4 Format = "ext4"
	// Erofs is an uncompressed, read-only EROFS image.
	Erofs Format = "erofs"
)
