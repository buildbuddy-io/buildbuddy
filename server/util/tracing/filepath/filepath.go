package filepath

import (
	"context"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/fs"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/os"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/span"

	iofs "io/fs"
)

const (
	Separator     = os.PathSeparator
	ListSeparator = os.PathListSeparator
)

var (
	ErrBadPattern       = filepath.ErrBadPattern
	SkipDir       error = fs.SkipDir
)

func Abs(ctx context.Context, path string) (string, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return filepath.Abs(path)
}
func Base(path string) string {
	return filepath.Base(path)
}
func Clean(path string) string {
	return filepath.Clean(path)
}
func Dir(path string) string {
	return filepath.Dir(path)
}
func EvalSymlinks(ctx context.Context, path string) (string, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return filepath.EvalSymlinks(path)
}
func Ext(path string) string {
	return filepath.Ext(path)
}
func FromSlash(path string) string {
	return filepath.FromSlash(path)
}
func Glob(ctx context.Context, pattern string) (matches []string, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return filepath.Glob(pattern)
}
func HasPrefix(p, prefix string) bool {
	return filepath.HasPrefix(p, prefix)
}
func IsAbs(path string) bool {
	return filepath.IsAbs(path)
}
func Join(elem ...string) string {
	return filepath.Join(elem...)
}
func Match(pattern, name string) (matched bool, err error) {
	return filepath.Match(pattern, name)
}
func Rel(basepath, targpath string) (string, error) {
	return filepath.Rel(basepath, targpath)
}
func Split(path string) (dir, file string) {
	return filepath.Split(path)
}
func SplitList(path string) []string {
	return filepath.SplitList(path)
}
func ToSlash(path string) string {
	return filepath.ToSlash(path)
}
func VolumeName(path string) string {
	return filepath.VolumeName(path)
}
func Walk(ctx context.Context, root string, fn WalkFunc) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return filepath.Walk(
		root,
		func(path string, info fs.FileInfo, err error) error {
			return fn(ctx, path, info, err)
		},
	)
}
func WalkDir(ctx context.Context, root string, fn fs.WalkDirFunc) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return filepath.WalkDir(
		root,
		func(path string, d iofs.DirEntry, err error) error {
			return fn(ctx, path, fs.CtxDirEntryWrapper(d), err)
		},
	)
}

type WalkFunc func(ctx context.Context, path string, info fs.FileInfo, err error) error
