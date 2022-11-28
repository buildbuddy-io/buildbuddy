package capabilities_server

import (
	"context"
	"math"

	"github.com/buildbuddy-io/buildbuddy/server/environment"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	smpb "github.com/buildbuddy-io/buildbuddy/proto/semver"
	remote_cache_config "github.com/buildbuddy-io/buildbuddy/server/remote_cache/config"
)

type CapabilitiesServer struct {
	supportCAS        bool
	supportRemoteExec bool
	supportZstd       bool
}

func Register(env environment.Env) error {
	// Register to handle GetCapabilities messages, which tell the client
	// that this server supports CAS functionality.
	env.SetCapabilitiesServer(NewCapabilitiesServer(
		/*supportCAS=*/ env.GetCache() != nil,
		/*supportRemoteExec=*/ env.GetRemoteExecutionService() != nil,
		/*supportZstd=*/ remote_cache_config.ZstdTranscodingEnabled(),
	))
	return nil
}

func NewCapabilitiesServer(supportCAS, supportRemoteExec, supportZstd bool) *CapabilitiesServer {
	return &CapabilitiesServer{
		supportCAS:        supportCAS,
		supportRemoteExec: supportRemoteExec,
		supportZstd:       supportZstd,
	}
}

func (s *CapabilitiesServer) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	c := repb.ServerCapabilities{
		// Support bazel 2.0 -> 99.9
		LowApiVersion:  &smpb.SemVer{Major: int32(2)},
		HighApiVersion: &smpb.SemVer{Major: int32(99), Minor: int32(9)},
	}
	var compressors []repb.Compressor_Value
	if s.supportZstd {
		compressors = []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD}
	}
	if s.supportCAS {
		c.CacheCapabilities = &repb.CacheCapabilities{
			DigestFunctions: []repb.DigestFunction_Value{repb.DigestFunction_SHA256},
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
