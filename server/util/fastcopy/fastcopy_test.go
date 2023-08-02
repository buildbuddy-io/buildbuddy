package fastcopy_test

import (
	"errors"
	"os"
	"path"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/fastcopy"
	"github.com/stretchr/testify/require"
)

func TestFastCopyDirFail(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	err := os.Mkdir(path.Join(ws, "foo"), 0o777)
	require.NoError(t, err)
	target := path.Join(ws, "bar")

	_, err = os.Stat(target)
	require.Error(t, err)
	require.True(t, errors.Is(err, os.ErrNotExist))

	err = fastcopy.FastCopy(ws, target)
	require.Error(t, err, "should not be able to copy a directory")
}

func TestFastCopyFile(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	source := testfs.MakeTempFile(t, ws, "foo")
	target := path.Join(ws, "bar")

	_, err := os.Stat(target)
	require.Error(t, err)
	require.True(t, errors.Is(err, os.ErrNotExist))

	err = fastcopy.FastCopy(source, target)
	require.NoError(t, err)

	_, err = os.Stat(target)
	require.NoError(t, err, "target file should exist")
}

func TestFastCopyFileExist(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	source := testfs.MakeTempFile(t, ws, "foo")
	target := testfs.MakeTempFile(t, ws, "bar")

	before, err := os.Stat(target)
	require.NoError(t, err)

	err = fastcopy.FastCopy(source, target)
	require.NoError(t, err)

	after, err := os.Stat(target)
	require.NoError(t, err)

	require.Equal(t, before.ModTime(), after.ModTime(), "target should not have been modified")
}

func TestFastCopySymlink(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	linkTarget := testfs.MakeTempFile(t, ws, "foo")
	source := path.Join(ws, "fooLink")
	err := os.Symlink(linkTarget, source)
	require.NoError(t, err)

	target := path.Join(ws, "bar")

	_, err = os.Stat(target)
	require.Error(t, err)
	require.True(t, errors.Is(err, os.ErrNotExist))

	err = fastcopy.FastCopy(source, target)
	require.NoError(t, err)

	_, err = os.Stat(target)
	require.NoError(t, err, "target symlink should exist")
}
