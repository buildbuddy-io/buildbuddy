//go:build windows && (amd64 || arm64)

package vfs_server

import (
	"context"
	"io/fs"

	"github.com/buildbuddy-io/buildbuddy/server/environment"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type LazyFile struct {
	Path  string
	Size  int64
	Perms fs.FileMode
}

type CASLazyFileProvider struct {
}

func NewCASLazyFileProvider(env environment.Env, ctx context.Context, remoteInstanceName string, digestFunction repb.DigestFunction_Value, inputTree *repb.Tree) (*CASLazyFileProvider, error) {
	panic("not implemented")
}

type Server struct {
}

func New(env environment.Env, workspacePath string) *Server {
	panic("not implemented")
}
