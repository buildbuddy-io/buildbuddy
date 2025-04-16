//go:build windows || (darwin && !ios)

package vfs_server

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
)

type Server struct {
}

func (s *Server) ComputeStats() *repb.VfsStats {
	return nil
}

func New(env environment.Env, workspacePath string) (*Server, error) {
	return nil, fmt.Errorf("not implemented")
}

func (p *Server) Prepare(ctx context.Context, layout *container.FileSystemLayout) error {
	return fmt.Errorf("Prepare not implemented")
}

func (p *Server) Stop() {

}

func (p *Server) Path() string {
	return ""
}

func NewDirectClient(server *Server) vfspb.FileSystemClient {
	return nil
}
