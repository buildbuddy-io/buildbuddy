package uffd

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type Handler struct{}

func NewHandler() (*Handler, error) {
	return &Handler{}, nil
}

// Start starts a goroutine to listen on the given socket path for Firecracker's
// UFFD initialization message, and then starts fulfilling UFFD requests using
// the given memory store.
func (h *Handler) Start(ctx context.Context, socketPath string, memoryStore *blockio.COWStore) error {
	return status.UnimplementedError("not yet implemented")
}

func (h *Handler) Stop() {
	// TODO
}
