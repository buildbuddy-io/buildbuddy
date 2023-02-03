package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/common/model"
	"golang.org/x/sync/errgroup"

	promapi "github.com/prometheus/client_golang/api/prometheus/v1"
)

const (
	// This exit code is returned if the metrics look unhealthy and the rollout should proceed
	metricsHealthyExitCode = 0
)

var (
	config     = flagutil.New("spec", Config{}, "Config to specify which metrics should be monitored by this script.")
	configPath = flag.String("config_path", "config.yaml", "Path to config file.")
)

// This script is intended to compare system health for a canary that has been deployed to the rest of the apps
// If the metrics are healthy, this script will return exit code 0 indicating that the rollout should proceed
// If the metrics are not healthy for the specified amount of time, the script will return an exit code indicating
// that the release should be rolled back
//
// Pre-requisites:
//   - You must create a tunnel to prometheus and port-forward in order to run this script with
//     `kubectl --context="" --namespace=<NAMESPACE> port-forward "<POD_NAME>" --address 0.0.0.0 <FROM_PORT>:<TO_PORT>`
func main() {
	flag.Parse()
	err := yaml.PopulateFlagsFromFile(*configPath)
	if err != nil {
		log.Fatalf("Could not read metrics config from %s: %s", *configPath, err)
	}
	err = log.Configure()
	if err != nil {
		log.Fatalf("Could not configure logger: %s", err)
	}
	prometheusClient, err := api.NewClient(api.Config{
		Address: config.PrometheusAddress,
	})
	if err != nil {
		log.Fatalf("Error creating prometheus client: %s\nDid you run the prometheus port-forward command?", err)
	}
	promAPI := promapi.NewAPI(prometheusClient)

	eg, gctx := errgroup.WithContext(context.Background())
	for _, metric := range config.PrometheusMetrics {
		metric := metric
		eg.Go(func() error {
			monitoringTimer := time.After(time.Duration(config.MonitoringTimeframeSeconds) * time.Second)
			monitoringTicker := time.NewTicker(time.Duration(metric.PollingIntervalSeconds) * time.Second)
			defer monitoringTicker.Stop()

			unhealthyCount := 0
			for {
				select {
				case <-gctx.Done():
					return nil
				case <-monitoringTimer:
					return nil
				case <-monitoringTicker.C:
					healthy, err := metricHealthy(promAPI, metric.Name, metric.CanaryQuery, metric.BaselineQuery, metric.HealthThreshold)
					if err != nil {
						log.Warningf("Error querying metric %s: %s", metric.Name, err)
					}

					if healthy {
						unhealthyCount = 0
					} else {
						unhealthyCount++
					}

					if unhealthyCount >= metric.MaxUnhealthyCount {
						return errors.New(fmt.Sprintf("%s metrics unhealthy.", metric.Name))
					}
				}
			}
		})
	}

	err = eg.Wait()
	if err != nil {
		log.Fatalf("Exiting metrics script: %s", err)
	}

	// If all metrics look healthy for the duration of the monitoring timeframe, continue with rollout
	os.Exit(metricsHealthyExitCode)
}

// Returns whether the given metric is healthy compared to the baseline
// If the metric's success rate differs from the baseline's by the input threshold, this will return false
// Otherwise will return true
func metricHealthy(promAPI promapi.API, metricName string, query string, baselineQuery string, healthThreshold float64) (bool, error) {
	ctx := context.Background()
	log.Debugf("Querying metric %s", metricName)

	result, _, err := promAPI.Query(ctx, query, time.Now())
	if err != nil {
		return false, err
	}
	resultVector := result.(model.Vector)
	metricValue := model.SampleValue(0)
	if len(resultVector) > 0 {
		metricValue = resultVector[0].Value
	}

	baselineResult, _, err := promAPI.Query(ctx, baselineQuery, time.Now())
	if err != nil {
		return false, err
	}
	baselineResultVector := baselineResult.(model.Vector)
	baselineMetricValue := model.SampleValue(0)
	if len(baselineResultVector) > 0 {
		baselineMetricValue = baselineResultVector[0].Value
	}

	if healthThreshold < 0 {
		// If the threshold is negative, the metric is unhealthy if it is too low
		return float64(baselineMetricValue-metricValue) > math.Abs(healthThreshold), nil
	}
	// If the threshold is positive, the metric is unhealthy if it is too high
	return float64(metricValue-baselineMetricValue) > math.Abs(healthThreshold), nil
}
