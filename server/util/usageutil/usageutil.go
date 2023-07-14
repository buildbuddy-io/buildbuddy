package usageutil

import (
	"context"
	"net"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"
)

var (
	originConfigs = flagutil.New("app.usage.origins", []*OriginConfig{}, "List of usage origin configs.")

	parseCIDRsOnce sync.Once
)

const (
	// UnknownLabelValue is used for label values that are not able to be
	// computed from the request.
	//
	// Note that we are intentionally not using an empty string here, since the
	// empty string is reserved to mean "missing data" in the DB. In other
	// words, empty string means that an app wrote the row before the label
	// value was supported. By contrast, "unknown" means that the app supports
	// the label but was not able to compute it from the request.
	UnknownLabelValue = "unknown"
)

type OriginConfig struct {
	// Label is the usage label for this origin.
	Label string `json:"label" yaml:"label"`

	// CIDRs is the list of CIDRs matching this label.
	// Ex: ["10.0.0.0/20"]
	CIDRs       []string `json:"cidrs" yaml:"cidrs"`
	parsedCIDRs []*net.IPNet

	// Header is the expected header value of x-buildbuddy-origin mapping to
	// this label.
	Header string `json:"header" yaml:"header"`
}

// Labels returns usage labels for the given request context.
func Labels(ctx context.Context) (*tables.UsageLabels, error) {
	return &tables.UsageLabels{
		Client: clientLabel(ctx),
		Origin: originLabel(ctx),
	}, nil
}

func clientLabel(ctx context.Context) string {
	// TODO: this func probably runs on most requests - profile this and make
	// sure it's not consuming excessive CPU/memory.
	md := bazel_request.GetRequestMetadata(ctx)
	if md == nil {
		return ""
	}
	if md.GetToolDetails().GetToolName() == "bazel" {
		return "bazel"
	}
	if md.GetExecutorDetails() != nil {
		return "executor"
	}
	return UnknownLabelValue
}

func originLabel(ctx context.Context) string {
	parseCIDRsOnce.Do(func() {
		for _, c := range *originConfigs {
			for _, raw := range c.CIDRs {
				_, n, err := net.ParseCIDR(raw)
				if err != nil {
					log.Errorf("Invalid CIDR for %q: %q: %s", c.Label, raw, err)
					continue
				}
				c.parsedCIDRs = append(c.parsedCIDRs, n)
			}
		}
	})
	if o := originFromClientIP(ctx); o != nil {
		return o.Label
	}
	if o := originFromHeader(ctx); o != nil {
		return o.Label
	}
	return UnknownLabelValue
}

func originFromClientIP(ctx context.Context) *OriginConfig {
	ip := net.ParseIP(clientip.Get(ctx))
	if ip == nil {
		return nil
	}
	for _, o := range *originConfigs {
		for _, cidr := range o.parsedCIDRs {
			if cidr.Contains(ip) {
				return o
			}
		}
	}
	return nil
}

func originFromHeader(ctx context.Context) *OriginConfig {
	md, _ := metadata.FromIncomingContext(ctx)
	if md == nil {
		return nil
	}
	vals, ok := md["x-buildbuddy-origin"]
	if !ok || len(vals) == 0 {
		return nil
	}
	for _, o := range *originConfigs {
		if o.Header == vals[0] {
			return o
		}
	}
	return nil
}
