package prom

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	promapi "github.com/prometheus/client_golang/api/prometheus/v1"
)

var (
	address = flag.String("prometheus.address", "", "the address of the promethus HTTP API")
)

type promQuerier struct {
	api promapi.API
}

func newQuerier() (*promQuerier, error) {
	if len(*address) == 0 {
		return nil, nil
	}
	c, err := api.NewClient(api.Config{
		Address: *address,
	})
	if err != nil {
		return nil, err
	}
	return &promQuerier{
		api: promapi.NewAPI(c),
	}, nil
}

type bbMetricsCollector struct {
	env             environment.Env
	groupID         string
	InvocationCount *prometheus.Desc
}

type metricsGroupRegistryManager struct {
	env               environment.Env
	registryByGroupID sync.Map // groupID -> registry
}

func newMetricsGroupRegistry(env environment.Env) *metricsGroupRegistryManager {
	return &metricsGroupRegistryManager{
		env:               env,
		registryByGroupID: sync.Map{},
	}
}

func Register(env environment.Env) error {
	q, err := newQuerier()
	if err != nil {
		return status.InternalErrorf("failed to configure prom querier: %s", err)
	}
	env.SetPromQuerier(q)

	m := newMetricsGroupRegistry(env)
	env.SetMetricsGroupRegistries(m)
	// TODO(lulu):UnregisterRegistries when shutdown.
	return nil
}

func (m *metricsGroupRegistryManager) GetOrCreateRegistry(groupID string) (*prometheus.Registry, error) {
	rInterface, loaded := m.registryByGroupID.LoadOrStore(groupID, prometheus.NewRegistry())
	reg, ok := rInterface.(*prometheus.Registry)
	if !ok {
		log.Errorf("unable to read prometheus registry for group ID %q", groupID)
	}
	if !loaded {
		err := reg.Register(newCollector(m.env, groupID))
		if err != nil {
			log.Errorf("unable to register prometheus registry for group ID %q:%s", groupID, err)
			return nil, err
		}
	}
	return reg, nil
}

func newCollector(env environment.Env, groupID string) *bbMetricsCollector {
	return &bbMetricsCollector{
		env:     env,
		groupID: groupID,
		InvocationCount: prometheus.NewDesc(
			"exported_builbuddy_invocation_count",
			"The total number of invocations whose logs were uploaded to buildbuddy.",
			[]string{
				metrics.InvocationStatusLabel,
				metrics.BazelExitCode,
				metrics.BazelCommand,
			},
			nil,
		),
	}
}

// Describe implements the prometheus.Collector interface
func (c *bbMetricsCollector) Describe(out chan<- *prometheus.Desc) {
	out <- c.InvocationCount
}

// Collect implements the prometheus.Collector interface
func (c *bbMetricsCollector) Collect(out chan<- prometheus.Metric) {
	promQuerier := c.env.GetPromQuerier()
	if promQuerier == nil {
		log.Error("prom querier not set up")
		return
	}
	metricsVec, err := promQuerier.FetchMetrics(c.env.GetServerContext(), c.groupID)
	if err != nil {
		log.Errorf("error fetch metrics: %v", err)
		return
	}
	for _, sample := range metricsVec {
		out <- prometheus.MustNewConstMetric(
			c.InvocationCount,
			prometheus.GaugeValue,
			float64(sample.Value),
			string(sample.Metric[metrics.InvocationStatusLabel]),
			string(sample.Metric[metrics.BazelExitCode]),
			string(sample.Metric[metrics.BazelCommand]))
	}
}

func (q *promQuerier) FetchMetrics(ctx context.Context, groupID string) (model.Vector, error) {
	query := fmt.Sprintf("sum by (bazel_command, bazel_exit_code, invocation_status)( buildbuddy_invocation_count{group_id='%s'})", groupID)
	result, _, err := q.api.Query(ctx, query, time.Now())
	if err != nil {
		return nil, err
	}
	resultVector := result.(model.Vector)
	return resultVector, nil
}
