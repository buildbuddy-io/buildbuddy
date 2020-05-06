package capabilities_server

import (
	"context"
	"math"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	smpb "github.com/buildbuddy-io/buildbuddy/proto/semver"
)

type CapabilitiesServer struct {
	supportCAS        bool
	supportRemoteExec bool
}

func NewCapabilitiesServer(supportCAS, supportRemoteExec bool) *CapabilitiesServer {
	return &CapabilitiesServer{
		supportCAS:        supportCAS,
		supportRemoteExec: supportRemoteExec,
	}
}

func (s *CapabilitiesServer) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	c := repb.ServerCapabilities{
		// Support bazel 2.0 -> 99.9
		LowApiVersion:  &smpb.SemVer{Major: int32(2)},
		HighApiVersion: &smpb.SemVer{Major: int32(99), Minor: int32(9)},
	}
	if s.supportCAS {
		c.CacheCapabilities = &repb.CacheCapabilities{
			DigestFunction: []repb.DigestFunction_Value{repb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &repb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			CachePriorityCapabilities: &repb.PriorityCapabilities{
				Priorities: []*repb.PriorityCapabilities_PriorityRange{
					{
						MinPriority: 0,
						MaxPriority: 0,
					},
				},
			},
			MaxBatchTotalSizeBytes:      10000000, // 10MB is the max amount you can fetch in one batch request.
			SymlinkAbsolutePathStrategy: repb.SymlinkAbsolutePathStrategy_ALLOWED,
		}
	}
	if s.supportRemoteExec {
		c.ExecutionCapabilities = &repb.ExecutionCapabilities{
			DigestFunction: repb.DigestFunction_SHA256,
			ExecEnabled:    true,
			ExecutionPriorityCapabilities: &repb.PriorityCapabilities{
				Priorities: []*repb.PriorityCapabilities_PriorityRange{
					{MinPriority: math.MinInt32, MaxPriority: math.MaxInt32},
				},
			},
		}
	}
	return &c, nil
}
