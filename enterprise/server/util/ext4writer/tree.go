package ext4writer

// Building an image directly from a REAPI input Tree + the filecache, without
// first materializing the inputs as a directory of hardlinks on the host.

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// FileOpener opens the content of an input file node (typically
// FileCache.Open). The returned file is closed by the writer.
type FileOpener func(ctx context.Context, node *repb.FileNode) (*os.File, error)

// TreeOptions configures DirectoryAndTreeToImage.
type TreeOptions struct {
	Options
	Tree           *repb.Tree
	DigestFunction repb.DigestFunction_Value
	Open           FileOpener
	// Now is used as the mtime of tree entries (REAPI has no timestamps).
}

// DirectoryAndTreeToImage builds an image containing everything under
// inputDir (e.g. pre-created output directories) plus the input Tree overlaid
// on it, with file contents read through opts.Open instead of from disk.
//
// Files with the same digest and executable bit share one inode (hard links),
// matching what the host workspace looks like today when inputs are hardlinked
// from the filecache.
func DirectoryAndTreeToImage(ctx context.Context, inputDir, outputFile string, opts *TreeOptions) (*Stats, error) {
	if opts == nil || opts.Tree == nil || opts.Open == nil {
		return nil, status.InvalidArgumentError("tree and opener are required")
	}
	w := &writer{opts: opts.Options, stats: &Stats{}}
	if w.opts.Concurrency <= 0 {
		w.opts.Concurrency = min(8, defaultConcurrency())
	}
	if w.opts.Now.IsZero() {
		w.opts.Now = time.Now()
	}
	w.open = func(n *node) (*os.File, error) {
		if n.fileNode != nil {
			return opts.Open(ctx, n.fileNode)
		}
		return os.Open(n.path)
	}
	start := time.Now()
	root, err := w.walk(inputDir)
	if err != nil {
		return nil, status.WrapError(err, "walk input directory")
	}
	if err := w.addTree(root, opts.Tree, opts.DigestFunction); err != nil {
		return nil, status.WrapError(err, "add input tree")
	}
	w.stats.WalkDuration = time.Since(start)
	return w.finish(ctx, outputFile)
}

type digestKey struct {
	hash string
	size int64
	exec bool
}

// addTree merges a REAPI Tree into the node tree rooted at root.
func (w *writer) addTree(root *node, tree *repb.Tree, df repb.DigestFunction_Value) error {
	dirs := make(map[string]*repb.Directory, len(tree.Children)+1)
	for _, d := range tree.Children {
		dg, err := digest.ComputeForMessage(d, df)
		if err != nil {
			return err
		}
		dirs[dg.GetHash()] = d
	}
	byDigest := map[digestKey]*node{}
	var add func(parent *node, dir *repb.Directory, depth int) error
	add = func(parent *node, dir *repb.Directory, depth int) error {
		if depth > 256 {
			return status.InvalidArgumentError("input tree too deep")
		}
		existing := map[string]*node{}
		for _, ch := range parent.children {
			existing[ch.name] = ch
		}
		// replaceHost drops a host-directory entry that an input of the same
		// name shadows: inputs win, matching the normal path where inputs are
		// written first and host-side additions (e.g. CI runner binaries)
		// skip names that already exist.
		replaceHost := func(name string) error {
			old, ok := existing[name]
			if !ok {
				return nil
			}
			if old.fileNode != nil || old.mode.IsDir() {
				return status.InvalidArgumentErrorf("duplicate entry %q", name)
			}
			for i, ch := range parent.children {
				if ch == old {
					parent.children = append(parent.children[:i], parent.children[i+1:]...)
					break
				}
			}
			delete(existing, name)
			switch {
			case old.mode.IsRegular():
				w.stats.Files--
				w.stats.DataBytes -= old.size
			case old.mode&os.ModeSymlink != 0:
				w.stats.Symlinks--
			}
			return nil
		}
		for _, f := range dir.GetFiles() {
			if err := replaceHost(f.GetName()); err != nil {
				return err
			}
			mode := uint32(0644)
			if f.GetIsExecutable() {
				mode = 0755
			}
			n := &node{name: f.GetName(), mode: os.FileMode(mode), rawMode: syscall.S_IFREG | mode, size: f.GetDigest().GetSizeBytes(), mtime: w.opts.Now, parent: parent, fileNode: f, links: 1}
			if n.size < 0 || n.size > maxFileBytes {
				return status.InvalidArgumentErrorf("%q: unsupported file size %d", f.GetName(), n.size)
			}
			key := digestKey{f.GetDigest().GetHash(), n.size, f.GetIsExecutable()}
			if orig, ok := byDigest[key]; ok && orig.links < 65000 {
				n.hardlink = orig
				orig.links++
				w.stats.Hardlinks++
				parent.children = append(parent.children, n)
				existing[n.name] = n
				continue
			}
			byDigest[key] = n
			w.stats.Files++
			w.stats.DataBytes += n.size
			parent.children = append(parent.children, n)
			existing[n.name] = n
		}
		for _, s := range dir.GetSymlinks() {
			if err := replaceHost(s.GetName()); err != nil {
				return err
			}
			n := &node{name: s.GetName(), mode: os.ModeSymlink | 0777, rawMode: syscall.S_IFLNK | 0777, target: s.GetTarget(), size: int64(len(s.GetTarget())), mtime: w.opts.Now, parent: parent, links: 1}
			w.stats.Symlinks++
			parent.children = append(parent.children, n)
			existing[n.name] = n
		}
		for _, d := range dir.GetDirectories() {
			child, ok := existing[d.GetName()]
			if ok {
				if !child.mode.IsDir() {
					return status.InvalidArgumentErrorf("%q exists and is not a directory", d.GetName())
				}
			} else {
				child = &node{name: d.GetName(), mode: os.ModeDir | 0755, rawMode: syscall.S_IFDIR | 0755, mtime: w.opts.Now, parent: parent}
				w.stats.Dirs++
				parent.children = append(parent.children, child)
				existing[child.name] = child
			}
			sub, ok := dirs[d.GetDigest().GetHash()]
			if !ok {
				return status.InvalidArgumentErrorf("directory %q (%s) missing from tree", d.GetName(), d.GetDigest().GetHash())
			}
			if err := add(child, sub, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := add(root, tree.GetRoot(), 0); err != nil {
		return err
	}
	// Directory entries must be sorted by name for deterministic images.
	var sortAll func(n *node)
	sortAll = func(n *node) {
		if n.mode.IsDir() {
			sortChildren(n)
			for _, ch := range n.children {
				sortAll(ch)
			}
		}
	}
	sortAll(root)
	return nil
}
