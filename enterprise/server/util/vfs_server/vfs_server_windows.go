//go:build windows && (amd64 || arm64)

package vfs_server

import (
	"github.com/buildbuddy-io/buildbuddy/server/environment"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type Server struct {
}

func (s *Server) UpdateIOStats(stats *repb.IOStats) {}

func New(env environment.Env, workspacePath string) *Server {
	panic("not implemented")
}
