package capabilities_server

import (
	"context"
	"math"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	smpb "github.com/buildbuddy-io/buildbuddy/proto/semver"
)

type CapabilitiesServer struct {
	supportCAS             bool
	supportRemoteExec      bool
	supportZstdCompression bool
}

func NewCapabilitiesServer(supportCAS, supportRemoteExec, supportZstdCompression bool) *CapabilitiesServer {
	return &CapabilitiesServer{
		supportCAS:             supportCAS,
		supportRemoteExec:      supportRemoteExec,
		supportZstdCompression: supportZstdCompression,
	}
}

func (s *CapabilitiesServer) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	c := repb.ServerCapabilities{
		// Support bazel 2.0 -> 99.9
		LowApiVersion:  &smpb.SemVer{Major: int32(2)},
		HighApiVersion: &smpb.SemVer{Major: int32(99), Minor: int32(9)},
	}
	if s.supportCAS {
		compressors := []repb.Compressor_Value{repb.Compressor_IDENTITY}
		if s.supportZstdCompression {
			compressors = append(compressors, repb.Compressor_ZSTD)
		}
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
			MaxBatchTotalSizeBytes:          0, // Default to protocol limit.
			SymlinkAbsolutePathStrategy:     repb.SymlinkAbsolutePathStrategy_ALLOWED,
			SupportedCompressors:            compressors,
			SupportedBatchUpdateCompressors: compressors,
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
