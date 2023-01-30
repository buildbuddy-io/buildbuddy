package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/common/model"
	"golang.org/x/sync/errgroup"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

const (
	// This exit code is returned if the metrics look unhealthy and the rollout should proceed
	metricsHealthyExitCode = 0

	// This error code is returned if the metrics look unhealthy and the canary should be rolled back
	metricsUnhealthyExitCode = 100
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
		fmt.Printf("Error creating prometheus client: %v\nDid you run the prometheus port-forward command?", err)
		os.Exit(1)
	}
	promAPI := v1.NewAPI(prometheusClient)

	eg := &errgroup.Group{}
	monitoringTimer := time.After(time.Duration(config.MonitoringTimeframeSeconds) * time.Second)
	scriptDurationComplete := false
	eg.Go(func() error {
		for {
			select {
			case <-monitoringTimer:
				scriptDurationComplete = true
				return nil
			}
		}
	})

	for _, metric := range config.PrometheusMetrics {
		metric := metric
		eg.Go(func() error {
			monitoringTicker := time.NewTicker(time.Duration(config.MonitoringTickerSeconds) * time.Second)
			defer monitoringTicker.Stop()

			for {
				select {
				case <-monitoringTicker.C:
					// Poll the metrics to make sure canary looks healthy
					sustainedUnhealthy, err := canarySustainedUnhealthy(
						promAPI, config.UnhealthyMonitoringTimeframeSeconds, metric.Name, metric.CanaryQuery, metric.BaselineQuery, metric.CanaryHealthThreshold)
					if err != nil {
						log.Fatalf("Error querying canary health: %s\nDid you run the prometheus port-forward command?", err)
					}
					if sustainedUnhealthy {
						log.Warningf("Metrics unhealthy for %s. Exiting script.", metric.Name)
						os.Exit(metricsUnhealthyExitCode)
					}
					break
				default:
					if scriptDurationComplete {
						return nil
					}
				}
			}
		})
	}

	eg.Wait()
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

// Returns whether the canary is unhealthy for a sustained amount of time
func canarySustainedUnhealthy(promAPI v1.API, unhealthyMonitoringTimeframeSec int, metricName string, canaryQuery string, baselineQuery string, canaryHealthThreshold float64) (bool, error) {
	healthy, err := canaryHealthy(promAPI, metricName, canaryQuery, baselineQuery, canaryHealthThreshold)
	if err != nil {
		return false, err
	} else if healthy {
		return false, nil
	}

	// For the timeout period, poll to check whether the canary's app liveness prober is still unhealthy
	timeout := time.After(time.Duration(unhealthyMonitoringTimeframeSec) * time.Second)
	pollLivenessTicker := time.NewTicker(30 * time.Second)
	defer pollLivenessTicker.Stop()

	for {
		select {
		case <-pollLivenessTicker.C:
			healthy, err = canaryHealthy(promAPI, metricName, canaryQuery, baselineQuery, canaryHealthThreshold)
			if err != nil {
				return false, err
			} else if healthy {
				return false, nil
			}
			break
		case <-timeout:
			// Canary has been unhealthy for the entirety of the unhealthy monitoring timeframe
			return true, nil
		}
	}
}
