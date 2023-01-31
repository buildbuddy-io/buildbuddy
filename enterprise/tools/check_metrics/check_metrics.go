package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/common/model"
	"golang.org/x/sync/errgroup"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
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
	prometheusClient, err := api.NewClient(api.Config{
		Address: config.PrometheusAddress,
	})
	if err != nil {
		log.Fatalf("Error creating prometheus client: %v\nDid you run the prometheus port-forward command?", err)
	}
	promAPI := v1.NewAPI(prometheusClient)

	eg, gctx := errgroup.WithContext(context.Background())
	for _, metric := range config.PrometheusMetrics {
		metric := metric
		eg.Go(func() error {
			monitoringTimer := time.After(time.Duration(config.MonitoringTimeframeSeconds) * time.Second)
			monitoringTicker := time.NewTicker(time.Duration(metric.PollingIntervalSeconds) * time.Second)
			defer monitoringTicker.Stop()

			for {
				select {
				case <-gctx.Done():
					return nil
				case <-monitoringTicker.C:
					r := retry.New(context.Background(), &retry.Options{
						InitialBackoff: time.Duration(metric.PollingIntervalSeconds) * time.Second,
						MaxRetries:     metric.MaxUnhealthyCount,
					})
					healthy := false
					for r.Next() {
						healthy, err = canaryHealthy(promAPI, metric.Name, metric.CanaryQuery, metric.BaselineQuery, metric.CanaryHealthThreshold)
						if err != nil {
							log.Warningf("Error querying metric %s: %s", metric.Name, err)
						} else if healthy {
							break
						}
					}

					// If metric was consecutively unhealthy for max number of retries, we should rollback
					if !healthy {
						return errors.New(fmt.Sprintf("%s metrics unhealthy.", metric.Name))
					}
					break
				case <-monitoringTimer:
					return nil
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

// Returns whether the given metric is healthy for the canary
// If the canary's success rate is lower than that of the other apps by the input threshold, this will return false
// Otherwise will return true
func canaryHealthy(v1api v1.API, metricName string, canaryQuery string, baselineQuery string, canaryHealthThreshold float64) (bool, error) {
	ctx := context.Background()
	log.Infof("Querying canary health for metric %s", metricName)

	canaryResult, _, err := v1api.Query(ctx, canaryQuery, time.Now())
	if err != nil {
		return false, err
	}
	canaryProberSuccessRate := canaryResult.(model.Vector)[0].Value

	baselineResult, _, err := v1api.Query(ctx, baselineQuery, time.Now())
	if err != nil {
		return false, err
	}
	baselineProberSuccessRate := baselineResult.(model.Vector)[0].Value

	// If the canary has lower success rate than the other apps by the set threshold, the canary is considered unhealthy
	if float64(baselineProberSuccessRate-canaryProberSuccessRate) > canaryHealthThreshold {
		return false, nil
	}
	return true, nil
}
