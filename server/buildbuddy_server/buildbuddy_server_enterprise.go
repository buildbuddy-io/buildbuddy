//go:build !buildbuddy_foss

package buildbuddy_server

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auditlog"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gcplink"
	remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"
	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
	gcpb "github.com/buildbuddy-io/buildbuddy/proto/gcp"
)

func getAuditLogResourceIDFromInvocation(id string) *alpb.ResourceID {
	return auditlog.InvocationResourceID(id)
}

func getAuditResourceIDFromGroupID(id string) *alpb.ResourceID {
	return auditlog.GroupResourceID(id)
}

func getAuditResourceIDFromSecret(sercretName string) *alpb.ResourceID {
	return auditlog.SecretResourceID(sercretName)
}

func (s *BuildBuddyServer) GetGCPProject(ctx context.Context, request *gcpb.GetGCPProjectRequest) (*gcpb.GetGCPProjectResponse, error) {
	return gcplink.GetGCPProject(s.env, ctx, request)
}

func remoteExecutionEnabled() bool {
	return remote_execution_config.RemoteExecutionEnabled()
}
