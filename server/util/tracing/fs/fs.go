package fs

import (
	"context"
	"io/fs"

	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/span"
)

var (
	ErrInvalid    = fs.ErrInvalid
	ErrPermission = fs.ErrPermission
	ErrExist      = fs.ErrExist
	ErrNotExist   = fs.ErrNotExist
	ErrClosed     = fs.ErrClosed

	SkipDir = fs.SkipDir
)

func Glob(ctx context.Context, fsys FS, pattern string) (matches []string, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return fs.Glob(FSWrapper(ctx, fsys), pattern)

}
func ReadFile(ctx context.Context, fsys FS, name string) ([]byte, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return fs.ReadFile(FSWrapper(ctx, fsys), name)
}
func ValidPath(name string) bool {
	return fs.ValidPath(name)
}
func WalkDir(ctx context.Context, fsys FS, root string, fn WalkDirFunc) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return fs.WalkDir(
		FSWrapper(ctx, fsys),
		root,
		func(path string, d fs.DirEntry, err error) error {
			return fn(ctx, path, CtxDirEntryWrapper(d), err)
		},
	)
}

type DirEntry interface {
	Name() string
	IsDir() bool
	Type() FileMode
	Info(ctx context.Context) (FileInfo, error)
}

type ctxDirEntryWrapper struct {
	fs.DirEntry
}

func CtxDirEntryWrapper(d fs.DirEntry) DirEntry {
	if d == nil {
		return nil
	}
	return &ctxDirEntryWrapper{d}
}
func (d *ctxDirEntryWrapper) unwrapDirEntry() fs.DirEntry {
	if d == nil {
		return nil
	}
	return d.DirEntry
}

func (d *ctxDirEntryWrapper) Name() string {
	return d.unwrapDirEntry().Name()
}
func (d *ctxDirEntryWrapper) IsDir() bool {
	return d.unwrapDirEntry().IsDir()
}
func (d *ctxDirEntryWrapper) Type() FileMode {
	return d.unwrapDirEntry().Type()
}
func (d *ctxDirEntryWrapper) Info(ctx context.Context) (FileInfo, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return d.unwrapDirEntry().Info()
}

func FileInfoToDirEntry(info FileInfo) DirEntry {
	return CtxDirEntryWrapper(fs.FileInfoToDirEntry(info))
}
func ReadDir(ctx context.Context, fsys FS, name string) ([]DirEntry, error) {
	_, spn := span.StartSpan(ctx)
	entries, err := fs.ReadDir(FSWrapper(ctx, fsys), name)
	spn.End()
	wrappedEntries := make([]DirEntry, len(entries))
	for i, e := range entries {
		wrappedEntries[i] = CtxDirEntryWrapper(e)
	}
	return wrappedEntries, err
}

type FS interface {
	Open(ctx context.Context, name string) (File, error)
}

type unwrapFS interface {
	unwrapFS() fs.FS
}

type unwrapCtxFS interface {
	unwrapFS() FS
}

type ctxFSWrapper struct {
	fs.FS
}

func CtxFSWrapper(f fs.FS) FS {
	if f == nil {
		return nil
	}
	return &ctxFSWrapper{f}
}
func (f *ctxFSWrapper) unwrapFS() fs.FS {
	if f == nil {
		return nil
	}
	return f.FS
}
func (f *ctxFSWrapper) Open(ctx context.Context, name string) (File, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	file, err := f.unwrapFS().Open(name)
	return CtxFileWrapper(file), err
}

type fsWrapper struct {
	FS
	ctx context.Context
}

func FSWrapper(ctx context.Context, f FS) fs.FS {
	if f == nil {
		return nil
	}
	return &fsWrapper{f, ctx}
}
func (f *fsWrapper) unwrapFS() FS {
	if f == nil {
		return nil
	}
	return f.FS
}
func (f *fsWrapper) Open(name string) (fs.File, error) {
	file, err := f.unwrapFS().Open(f.ctx, name)
	return FileWrapper(f.ctx, file), err
}

func Sub(ctx context.Context, fsys FS, dir string) (FS, error) {
	if fsys, ok := fsys.(unwrapFS); ok {
		unwrapped := fsys.unwrapFS()
		_, spn := span.StartSpan(ctx)
		sub, err := fs.Sub(unwrapped, dir)
		spn.End()
		return CtxFSWrapper(sub), err
	}
	wrapped := FSWrapper(ctx, fsys)
	_, spn := span.StartSpan(ctx)
	sub, err := fs.Sub(wrapped, dir)
	spn.End()
	return CtxFSWrapper(sub), err
}

type File interface {
	Stat(ctx context.Context) (FileInfo, error)
	Read(ctx context.Context, p []byte) (int, error)
	Close(ctx context.Context) error
}

type ctxFileWrapper struct {
	fs.File
}

func CtxFileWrapper(f fs.File) File {
	if f == nil {
		return nil
	}
	return &ctxFileWrapper{f}
}
func (f *ctxFileWrapper) unwrapFile() fs.File {
	if f == nil {
		return nil
	}
	return f.File
}
func (f *ctxFileWrapper) Stat(ctx context.Context) (FileInfo, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.unwrapFile().Stat()
}
func (f *ctxFileWrapper) Read(ctx context.Context, p []byte) (int, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.unwrapFile().Read(p)
}
func (f *ctxFileWrapper) Close(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.unwrapFile().Close()
}

type fileWrapper struct {
	File
	ctx context.Context
}

func FileWrapper(ctx context.Context, f File) fs.File {
	if f == nil {
		return nil
	}
	return &fileWrapper{f, ctx}
}
func (f *fileWrapper) unwrapFile() File {
	if f == nil {
		return nil
	}
	return f.File
}
func (f *fileWrapper) Stat() (fs.FileInfo, error) {
	return f.unwrapFile().Stat(f.ctx)
}
func (f *fileWrapper) Read(p []byte) (int, error) {
	return f.unwrapFile().Read(f.ctx, p)
}
func (f *fileWrapper) Close() error {
	return f.unwrapFile().Close(f.ctx)
}

type FileInfo = fs.FileInfo

func Stat(ctx context.Context, fsys FS, name string) (FileInfo, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return fs.Stat(FSWrapper(ctx, fsys), name)
}

type FileMode = fs.FileMode
type GlobFS interface {
	FS
	Glob(ctx context.Context, pattern string) ([]string, error)
}
type PathError = fs.PathError
type ReadDirFS interface {
	FS
	ReadDir(ctx context.Context, name string) ([]DirEntry, error)
}
type ReadDirFile interface {
	File
	ReadDir(ctx context.Context, n int) ([]DirEntry, error)
}
type ReadFileFS interface {
	FS
	ReadFile(ctx context.Context, name string) ([]byte, error)
}
type StatFS interface {
	FS
	Stat(ctx context.Context, name string) (FileInfo, error)
}
type SubFS interface {
	FS
	Sub(ctx context.Context, dir string) (FS, error)
}
type WalkDirFunc func(ctx context.Context, path string, d DirEntry, err error) error
