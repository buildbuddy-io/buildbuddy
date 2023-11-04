//go:build buildbuddy_foss

package buildbuddy_server

import (
	"context"

	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
	gcpb "github.com/buildbuddy-io/buildbuddy/proto/gcp"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func getAuditLogResourceIDFromInvocation(id string) *alpb.ResourceID {
	return nil
}

func getAuditResourceIDFromGroupID(id string) *alpb.ResourceID {
	return nil
}

func getAuditResourceIDFromSecret(sercretName string) *alpb.ResourceID {
	return nil
}

func (s *BuildBuddyServer) GetGCPProject(ctx context.Context, request *gcpb.GetGCPProjectRequest) (*gcpb.GetGCPProjectResponse, error) {
	return nil, status.UnimplementedError("BuildBuddy-FOSS does not have gcp project")
}

func remoteExecutionEnabled() bool {
	return false
}
