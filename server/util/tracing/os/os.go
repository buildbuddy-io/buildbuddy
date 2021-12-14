package os

import (
	"context"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/fs"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/span"
)

const (
	O_RDONLY          = os.O_RDONLY
	O_WRONLY          = os.O_WRONLY
	O_RDWR            = os.O_RDWR
	O_APPEND          = os.O_APPEND
	O_CREATE          = os.O_CREATE
	O_EXCL            = os.O_EXCL
	O_SYNC            = os.O_SYNC
	O_TRUNC           = os.O_TRUNC
	SEEK_SET          = os.SEEK_SET
	SEEK_CUR          = os.SEEK_CUR
	SEEK_END          = os.SEEK_END
	PathSeparator     = os.PathSeparator
	PathListSeparator = os.PathListSeparator
	ModeDir           = os.ModeDir
	ModeAppend        = os.ModeAppend
	ModeExclusive     = os.ModeExclusive
	ModeTemporary     = os.ModeTemporary
	ModeSymlink       = os.ModeSymlink
	ModeDevice        = os.ModeDevice
	ModeNamedPipe     = os.ModeNamedPipe
	ModeSocket        = os.ModeSocket
	ModeSetuid        = os.ModeSetuid
	ModeSetgid        = os.ModeSetgid
	ModeCharDevice    = os.ModeCharDevice
	ModeSticky        = os.ModeSticky
	ModeIrregular     = os.ModeIrregular
	ModeType          = os.ModeType
	ModePerm          = os.ModePerm
	DevNull           = os.DevNull
)

var (
	ErrInvalid                 = os.ErrInvalid
	ErrPermission              = os.ErrPermission
	ErrExist                   = os.ErrExist
	ErrNotExist                = os.ErrNotExist
	ErrClosed                  = os.ErrClosed
	ErrNoDeadline              = os.ErrNoDeadline
	ErrDeadlineExceeded        = os.ErrDeadlineExceeded
	Stdin                      = os.Stdin
	Stdout                     = os.Stdout
	Stderr                     = os.Stderr
	Args                       = os.Args
	ErrProcessDone             = os.ErrProcessDone
	Interrupt           Signal = syscall.SIGINT
	Kill                Signal = syscall.SIGKILL
)

type DirEntry = fs.DirEntry

func ReadDir(ctx context.Context, name string) ([]DirEntry, error) {
	_, spn := span.StartSpan(ctx)
	entries, err := os.ReadDir(name)
	spn.End()
	if entries == nil {
		return nil, err
	}
	wrappedEntries := make([]DirEntry, len(entries))
	for i, e := range entries {
		wrappedEntries[i] = fs.CtxDirEntryWrapper(e)
	}
	return wrappedEntries, err
}

func newFile(f *os.File) *File {
	if f == nil {
		return nil
	}
	return &File{f}
}

type File struct {
	*os.File
}

func Create(ctx context.Context, name string) (*File, error) {
	_, spn := span.StartSpan(ctx)
	f, err := os.Create(name)
	spn.End()
	return newFile(f), err
}

func CreateTemp(ctx context.Context, dir, pattern string) (*File, error) {
	_, spn := span.StartSpan(ctx)
	f, err := os.CreateTemp(dir, pattern)
	spn.End()
	return newFile(f), err
}

func NewFile(ctx context.Context, fd uintptr, name string) *File {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return newFile(os.NewFile(fd, name))
}

func Open(ctx context.Context, name string) (*File, error) {
	_, spn := span.StartSpan(ctx)
	f, err := os.Open(name)
	spn.End()
	return &File{f}, err
}

func OpenFile(ctx context.Context, name string, flag int, perm FileMode) (*File, error) {
	_, spn := span.StartSpan(ctx)
	f, err := os.OpenFile(name, flag, perm)
	spn.End()
	return &File{f}, err
}

func (f *File) file() *os.File {
	if f == nil {
		return nil
	}
	return f.File
}

func (f *File) Chdir() error {
	return f.file().Chdir()
}

func (f *File) Chmod(ctx context.Context, mode FileMode) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Chmod(mode)
}

func (f *File) Chown(ctx context.Context, uid, gid int) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Chown(uid, gid)
}

func (f *File) Close(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Close()
}

func (f *File) Fd() uintptr {
	return f.file().Fd()
}

func (f *File) Name() string {
	return f.file().Name()
}

func (f *File) Read(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Read(b)
}

func (f *File) ReadAt(ctx context.Context, b []byte, off int64) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().ReadAt(b, off)
}

func (f *File) ReadDir(ctx context.Context, n int) ([]DirEntry, error) {
	_, spn := span.StartSpan(ctx)
	entries, err := f.file().ReadDir(n)
	spn.End()
	if entries == nil {
		return nil, err
	}
	wrappedEntries := make([]DirEntry, len(entries))
	for i, e := range entries {
		wrappedEntries[i] = fs.CtxDirEntryWrapper(e)
	}
	return wrappedEntries, err
}

func (f *File) ReadFrom(ctx context.Context, r io.Reader) (n int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().ReadFrom(r)
}

func (f *File) Readdir(ctx context.Context, n int) ([]FileInfo, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Readdir(n)
}

func (f *File) Readdirnames(ctx context.Context, n int) (names []string, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Readdirnames(n)
}

func (f *File) Seek(ctx context.Context, offset int64, whence int) (ret int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Seek(offset, whence)
}

func (f *File) SetDeadline(t time.Time) error {
	return f.file().SetDeadline(t)
}

func (f *File) SetReadDeadline(t time.Time) error {
	return f.file().SetReadDeadline(t)
}

func (f *File) SetWriteDeadline(t time.Time) error {
	return f.file().SetWriteDeadline(t)
}

func (f *File) Stat(ctx context.Context) (FileInfo, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Stat()
}

func (f *File) Sync(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Sync()
}

func (f *File) SyscallConn() (syscall.RawConn, error) {
	return f.file().SyscallConn()
}

func (f *File) Truncate(ctx context.Context, size int64) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Truncate(size)
}

func (f *File) Write(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().Write(b)
}

func (f *File) WriteAt(ctx context.Context, b []byte, off int64) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().WriteAt(b, off)
}

func (f *File) WriteString(ctx context.Context, s string) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return f.file().WriteString(s)
}

type FileInfo = fs.FileInfo

func Lstat(ctx context.Context, name string) (FileInfo, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Lstat(name)
}

func Stat(ctx context.Context, name string) (FileInfo, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Stat(name)
}

type FileMode = fs.FileMode

type LinkError = os.LinkError
type PathError = os.PathError

type ProcAttr = os.ProcAttr

type Process struct {
	*os.Process
}

func FindProcess(ctx context.Context, pid int) (*Process, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	p, err := os.FindProcess(pid)
	if p == nil {
		return nil, err
	}
	return &Process{p}, err
}

func StartProcess(ctx context.Context, name string, argv []string, attr *ProcAttr) (*Process, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	p, err := os.StartProcess(name, argv, attr)
	if p == nil {
		return nil, err
	}
	return &Process{p}, err
}

func (p *Process) Kill(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return p.Process.Kill()
}

func (p *Process) Release(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return p.Process.Release()
}

func (p *Process) Signal(ctx context.Context, sig Signal) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return p.Process.Signal(sig)
}

func (p *Process) Wait(ctx context.Context) (*ProcessState, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return p.Process.Wait()
}

type ProcessState = os.ProcessState

type Signal = os.Signal

type SyscallError = os.SyscallError

func Chdir(ctx context.Context, dir string) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Chdir(dir)
}

func Chmod(ctx context.Context, name string, mode FileMode) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Chmod(name, mode)
}

func Chown(ctx context.Context, name string, uid, gid int) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Chown(name, uid, gid)
}

func Chtimes(ctx context.Context, name string, atime time.Time, mtime time.Time) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Chtimes(name, atime, mtime)
}

func Clearenv() {
	os.Clearenv()
}

func DirFS(dir string) fs.FS {
	return fs.CtxFSWrapper(os.DirFS(dir))
}

func Environ() []string {
	return os.Environ()
}

func Executable() (string, error) {
	return os.Executable()
}

func Exit(code int) {
	os.Exit(code)
}

func Expand(s string, mapping func(string) string) string {
	return os.Expand(s, mapping)
}

func ExpandEnv(s string) string {
	return os.ExpandEnv(s)
}

func Getegid() int {
	return os.Getegid()
}

func Getenv(key string) string {
	return os.Getenv(key)
}

func Geteuid() int {
	return os.Geteuid()
}

func Getgid() int {
	return os.Getgid()
}

func Getgroups() ([]int, error) {
	return os.Getgroups()
}

func Getpagesize() int {
	return os.Getpagesize()
}

func Getpid() int {
	return os.Getpid()
}

func Getppid() int {
	return os.Getppid()
}

func Getuid() int {
	return os.Getuid()
}

func Getwd(ctx context.Context) (dir string, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Getwd()
}

func Hostname(ctx context.Context) (name string, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Hostname()
}

func IsExist(err error) bool {
	return os.IsExist(err)
}

func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

func IsPathSeparator(c uint8) bool {
	return os.IsPathSeparator(c)
}

func IsPermission(err error) bool {
	return os.IsPermission(err)
}

func IsTimeout(err error) bool {
	return os.IsTimeout(err)
}

func Lchown(ctx context.Context, name string, uid, gid int) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Lchown(name, uid, gid)
}

func Link(ctx context.Context, oldname, newname string) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Link(oldname, newname)
}

func LookupEnv(key string) (string, bool) {
	return os.LookupEnv(key)
}

func Mkdir(ctx context.Context, name string, perm FileMode) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Mkdir(name, perm)
}

func MkdirAll(ctx context.Context, path string, perm FileMode) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.MkdirAll(path, perm)
}

func MkdirTemp(ctx context.Context, dir, pattern string) (string, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.MkdirTemp(dir, pattern)
}

func NewSyscallError(syscall string, err error) error {
	return os.NewSyscallError(syscall, err)
}

func Pipe() (*File, *File, error) {
	r, w, err := os.Pipe()
	return newFile(r), newFile(w), err
}

func ReadFile(ctx context.Context, name string) ([]byte, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.ReadFile(name)
}

func Readlink(ctx context.Context, name string) (string, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Readlink(name)
}

func Remove(ctx context.Context, name string) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Remove(name)
}

func RemoveAll(ctx context.Context, path string) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.RemoveAll(path)
}

func Rename(ctx context.Context, oldpath, newpath string) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Rename(oldpath, newpath)
}

func SameFile(fi1, fi2 FileInfo) bool {
	return os.SameFile(fi1, fi2)
}

func Setenv(key, value string) error {
	return os.Setenv(key, value)
}

func Symlink(ctx context.Context, oldname, newname string) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Symlink(oldname, newname)
}

func TempDir(ctx context.Context) string {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.TempDir()
}

func Truncate(ctx context.Context, name string, size int64) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.Truncate(name, size)
}

func Unsetenv(key string) error {
	return os.Unsetenv(key)
}

func UserCacheDir() (string, error) {
	return os.UserCacheDir()
}

func UserConfigDir() (string, error) {
	return os.UserConfigDir()
}

func UserHomeDir() (string, error) {
	return os.UserHomeDir()
}

func WriteFile(ctx context.Context, name string, data []byte, perm FileMode) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return os.WriteFile(name, data, perm)
}
