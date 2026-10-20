//go:build linux

// Package vext4 builds ext4 workspace images without copying input file
// contents into them.
//
// The image file initially contains only filesystem metadata. [Store] serves
// input data from separate source files as it is read. Writes go to the
// image file, preserving the original inputs.
//
// [Build] reads an existing directory. [BuildTree] uses a remote execution
// input tree and sources supplied by the caller.
//
// The package is split by stage. tree.go turns the directory or input tree
// into inodes, layout.go sizes the image and assigns every block, encode.go
// turns the ext4 on-disk structures into bytes, write.go writes them to the
// image, and store.go serves the result.
package vext4

import (
	"context"
	"fmt"
	"os"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Build writes an ext4 image of the directory tree at dir to imagePath. The
// image holds the tree's metadata only, and each file's data blocks are
// virtual, served from the file under dir rather than copied. freeBytes is the space left free
// for files the guest creates. The returned Store must be closed by the
// caller, which also owns the image file.
func Build(ctx context.Context, dir, imagePath string, freeBytes int64) (*Store, error) {
	inodes, err := walkDir(ctx, dir)
	if err != nil {
		return nil, err
	}
	return build(ctx, inodes, imagePath, freeBytes)
}

// BuildTree is like Build, but takes the tree from a remote execution input
// tree, with each file's data blocks served from the Source that
// spec.Source returns for it. The Store opens sources with ctx, so ctx must stay valid
// for as long as the Store is served.
func BuildTree(ctx context.Context, spec *Tree, imagePath string, freeBytes int64) (*Store, error) {
	inodes, err := walkInputTree(spec)
	if err != nil {
		return nil, err
	}
	return build(ctx, inodes, imagePath, freeBytes)
}

// build lays out the tree, writes the image and returns its Store.
func build(ctx context.Context, inodes inodeList, imagePath string, freeBytes int64) (*Store, error) {
	l, err := layout(inodes, freeBytes)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(imagePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("create image: %w", err)
	}
	if err := f.Truncate(l.sizeBytes()); err != nil {
		f.Close()
		os.Remove(imagePath)
		return nil, fmt.Errorf("size image: %w", err)
	}
	if err := write(f, l, inodes); err != nil {
		f.Close()
		os.Remove(imagePath)
		return nil, err
	}
	return newStore(ctx, f, l, inodes), nil
}

// Tree is what BuildTree builds an image from. It holds a remote execution
// input tree, directories to add to it, and where each file's contents come
// from.
type Tree struct {
	Inputs         *repb.Tree
	DigestFunction repb.DigestFunction_Value
	// Dirs are directories to add to the tree, as slash-separated paths
	// relative to its root, such as the directories an action writes its
	// outputs to. Missing parents are added as well.
	Dirs []string
	// Source returns where the contents of the file at path, relative to
	// the tree's root, come from. It is called once per distinct digest:
	// files with the same contents share a Source.
	Source func(path string, node *repb.FileNode) Source
}

// Stats counts what a Store serves and what the guest has done with it so
// far.
type Stats struct {
	Files, Directories, Symlinks int
	// FileBytes is the total size of the files, and VirtualBytes the size
	// of their virtual data blocks.
	FileBytes, VirtualBytes int64
	// FilesRead is the number of files the guest has read any part of, and
	// ReadFileBytes their total size. Files that share a Source count once.
	FilesRead     int
	ReadFileBytes int64
	// DirtyBytes is the size of the virtual blocks the guest has overwritten.
	DirtyBytes int64
	// SourceReadBytes is the number of bytes read from sources.
	SourceReadBytes int64
}
